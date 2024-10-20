from abc import ABC, abstractmethod
from typing import Dict, Any
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import asyncio, requests, re, os
from .config import *
from bs4 import element, NavigableString, Comment
from urllib.parse import urljoin
from requests.exceptions import InvalidSchema

from .utils import (
    sanitize_input_encode,
    sanitize_html,
    extract_metadata,
    InvalidCSSSelectorError,
    CustomHTML2Text
)

class ContentScrappingStrategy(ABC):
    @abstractmethod
    def scrap(self, url: str, html: str, **kwargs) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def ascrap(self, url: str, html: str, **kwargs) -> Dict[str, Any]:
        pass

class WebScrappingStrategy(ContentScrappingStrategy):
    def scrap(self, url: str, html: str, **kwargs) -> Dict[str, Any]:
        return self._get_content_of_website_optimized(url, html, is_async=False, **kwargs)

    async def ascrap(self, url: str, html: str, **kwargs) -> Dict[str, Any]:
        return await asyncio.to_thread(self._get_content_of_website_optimized, url, html, **kwargs)

    def _get_content_of_website_optimized(self, url: str, html: str, word_count_threshold: int = MIN_WORD_THRESHOLD, css_selector: str = None, **kwargs) -> Dict[str, Any]:
        success = True
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        body = soup.body
        
        
        image_description_min_word_threshold = kwargs.get('image_description_min_word_threshold', IMAGE_DESCRIPTION_MIN_WORD_THRESHOLD)

        for tag in kwargs.get('excluded_tags', []) or []:
            for el in body.select(tag):
                el.decompose()
        
        if css_selector:
            selected_elements = body.select(css_selector)
            if not selected_elements:
                return {
                    'markdown': '',
                    'cleaned_html': '',
                    'success': True,
                    'media': {'images': [], 'videos': [], 'audios': []},
                    'links': {'internal': [], 'external': []},
                    'metadata': {},
                    'message': f"No elements found for CSS selector: {css_selector}"
                }
                # raise InvalidCSSSelectorError(f"Invalid CSS selector, No elements found for CSS selector: {css_selector}")
            body = soup.new_tag('div')
            for el in selected_elements:
                body.append(el)

        links = {'internal': [], 'external': []}
        media = {'images': [], 'videos': [], 'audios': []}

        # Extract meaningful text for media files from closest parent
        def find_closest_parent_with_useful_text(tag):
                current_tag = tag
                while current_tag:
                    current_tag = current_tag.parent
                    # Get the text content of the parent tag
                    if current_tag:
                        text_content = current_tag.get_text(separator=' ',strip=True)
                        # Check if the text content has at least word_count_threshold
                        if len(text_content.split()) >= image_description_min_word_threshold:
                            return text_content
                return None

        def process_image(img, url, index, total_images):
            #Check if an image has valid display and inside undesired html elements
            def is_valid_image(img, parent, parent_classes):
                style = img.get('style', '')
                src = img.get('src', '')
                classes_to_check = ['button', 'icon', 'logo']
                tags_to_check = ['button', 'input']
                return all([
                    'display:none' not in style,
                    src,
                    not any(s in var for var in [src, img.get('alt', ''), *parent_classes] for s in classes_to_check),
                    parent.name not in tags_to_check
                ])

            #Score an image for it's usefulness
            def score_image_for_usefulness(img, base_url, index, images_count):
                # Function to parse image height/width value and units
                def parse_dimension(dimension):
                    if dimension:
                        match = re.match(r"(\d+)(\D*)", dimension)
                        if match:
                            number = int(match.group(1))
                            unit = match.group(2) or 'px'  # Default unit is 'px' if not specified
                            return number, unit
                    return None, None

                # Fetch image file metadata to extract size and extension
                def fetch_image_file_size(img, base_url):
                    #If src is relative path construct full URL, if not it may be CDN URL
                    img_url = urljoin(base_url,img.get('src'))
                    try:
                        response = requests.head(img_url)
                        if response.status_code == 200:
                            return response.headers.get('Content-Length',None)
                        else:
                            print(f"Failed to retrieve file size for {img_url}")
                            return None
                    except InvalidSchema as e:
                        return None
                    finally:
                        return

                image_height = img.get('height')
                height_value, height_unit = parse_dimension(image_height)
                image_width =  img.get('width')
                width_value, width_unit = parse_dimension(image_width)
                image_size = 0 #int(fetch_image_file_size(img,base_url) or 0)
                image_format = os.path.splitext(img.get('src',''))[1].lower()
                # Remove . from format
                image_format = image_format.strip('.').split('?')[0]
                score = 0
                if height_value:
                    if height_unit == 'px' and height_value > 150:
                        score += 1
                    if height_unit in ['%','vh','vmin','vmax'] and height_value >30:
                        score += 1
                if width_value:
                    if width_unit == 'px' and width_value > 150:
                        score += 1
                    if width_unit in ['%','vh','vmin','vmax'] and width_value >30:
                        score += 1
                if image_size > 10000:
                    score += 1
                if img.get('alt') != '':
                    score+=1
                if any(image_format==format for format in ['jpg','png','webp']):
                    score+=1
                if index/images_count<0.5:
                    score+=1
                return score

            
            
            if not is_valid_image(img, img.parent, img.parent.get('class', [])):
                return None
            score = score_image_for_usefulness(img, url, index, total_images)
            if score <= IMAGE_SCORE_THRESHOLD:
                return None
            return {
                'src': img.get('src', ''),
                'data-src': img.get('data-src', ''),
                'alt': img.get('alt', ''),
                'desc': find_closest_parent_with_useful_text(img),
                'score': score,
                'type': 'image'
            }

        def remove_unwanted_attributes(element, important_attrs, keep_data_attributes=False):
            attrs_to_remove = []
            for attr in element.attrs:
                if attr not in important_attrs:
                    if keep_data_attributes:
                        if not attr.startswith('data-'):
                            attrs_to_remove.append(attr)
                    else:
                        attrs_to_remove.append(attr)
            
            for attr in attrs_to_remove:
                del element[attr]
        
        def process_element(element: element.PageElement) -> bool:
            try:
                if isinstance(element, NavigableString):
                    if isinstance(element, Comment):
                        element.extract()
                    return False
                
                # if element.name == 'img':
                #     process_image(element, url, 0, 1)
                #     return True

                if element.name in ['script', 'style', 'link', 'meta', 'noscript']:
                    element.decompose()
                    return False

                keep_element = False

                if element.name == 'a' and element.get('href'):
                    href = element['href']
                    url_base = url.split('/')[2]
                    link_data = {'href': href, 'text': element.get_text()}
                    if href.startswith('http') and url_base not in href:
                        links['external'].append(link_data)
                    else:
                        links['internal'].append(link_data)
                    keep_element = True
                    
                    if kwargs.get('exclude_external_links', True):
                        href_url_base = href.split('/')[2]
                        if url_base not in href_url_base:
                            element.decompose()
                            return False
                        
                    # Check if we should esclude links to all major social media platforms
                    if not kwargs.get('exclude_external_links', False) and kwargs.get('exclude_social_media_links', True):
                        social_media_domains = SOCIAL_MEDIA_DOMAINS + kwargs.get('social_media_domains', [])
                        social_media_domains = list(set(social_media_domains))
                        if any(domain in href for domain in social_media_domains):
                            element.decompose()
                            return False

                elif element.name == 'img':
                    # Check flag if we should remove external images
                    if kwargs.get('exclude_external_images', False):
                        src = element.get('src', '')
                        src_url_base = src.split('/')[2]
                        url_base = url.split('/')[2]
                        if url_base not in src_url_base:
                            element.decompose()
                            return False
                        
                    if not kwargs.get('exclude_external_images', False) and kwargs.get('exclude_social_media_links', True):
                        src = element.get('src', '')
                        src_url_base = src.split('/')[2]
                        url_base = url.split('/')[2]
                        if any(domain in src for domain in SOCIAL_MEDIA_DOMAINS):
                            element.decompose()
                            return False
                    
                    return True  # Always keep image elements

                elif element.name in ['video', 'audio']:
                    media[f"{element.name}s"].append({
                        'src': element.get('src'),
                        'alt': element.get('alt'),
                        'type': element.name,
                        'description': find_closest_parent_with_useful_text(element)
                    })
                    source_tags = element.find_all('source')
                    for source_tag in source_tags:
                        media[f"{element.name}s"].append({
                        'src': source_tag.get('src'),
                        'alt': element.get('alt'),
                        'type': element.name,
                        'description': find_closest_parent_with_useful_text(element)
                    })
                    return True  # Always keep video and audio elements

                if element.name in ONLY_TEXT_ELIGIBLE_TAGS:
                    if kwargs.get('only_text', False):
                        element.replace_with(element.get_text())

                remove_unwanted_attributes(element, IMPORTANT_ATTRS, kwargs.get('keep_data_attributes', False))
                # for attr in element.attrs:
                #     if attr not in IMPORTANT_ATTRS or (attr.startswith('data-') and not kwargs.get('keep_data_attributes', False)):
                #         del element[attr]                            

                # Print element name and attributes
                print(element.name, element.attrs)

                # Process children
                for child in list(element.children):
                    if isinstance(child, NavigableString) and not isinstance(child, Comment):
                        if len(child.strip()) > 0:
                            keep_element = True
                    else:
                        if process_element(child):
                            keep_element = True
                    

                # Check word count
                if not keep_element:
                    word_count = len(element.get_text(strip=True).split())
                    keep_element = word_count >= word_count_threshold

                if not keep_element:
                    element.decompose()

                return keep_element
            except Exception as e:
                print('Error processing element:', str(e))
                return False

        #process images by filtering and extracting contextual text from the page
        # imgs = body.find_all('img')
        # media['images'] = [
        #     result for result in
        #     (process_image(img, url, i, len(imgs)) for i, img in enumerate(imgs))
        #     if result is not None
        # ]
        
        process_element(body)

        # # Process images using ThreadPoolExecutor
        
        
        
        imgs = body.find_all('img')
        
        with ThreadPoolExecutor() as executor:
            image_results = list(executor.map(process_image, imgs, [url]*len(imgs), range(len(imgs)), [len(imgs)]*len(imgs)))
        media['images'] = [result for result in image_results if result is not None]

        def flatten_nested_elements(node):
            if isinstance(node, NavigableString):
                return node
            if len(node.contents) == 1 and isinstance(node.contents[0], element.Tag) and node.contents[0].name == node.name:
                return flatten_nested_elements(node.contents[0])
            node.contents = [flatten_nested_elements(child) for child in node.contents]
            return node

        body = flatten_nested_elements(body)
        base64_pattern = re.compile(r'data:image/[^;]+;base64,([^"]+)')
        for img in imgs:
            src = img.get('src', '')
            if base64_pattern.match(src):
                # Replace base64 data with empty string
                img['src'] = base64_pattern.sub('', src)
                
        try:
            str(body)
        except Exception as e:
            # Reset body to the original HTML
            success = False
            body = BeautifulSoup(html, 'html.parser')
            
            # Create a new div with a special ID
            error_div = body.new_tag('div', id='crawl4ai_error_message')
            error_div.string = '''
            Crawl4AI Error: This page is not fully supported.
            
            Possible reasons:
            1. The page may have restrictions that prevent crawling.
            2. The page might not be fully loaded.
            
            Suggestions:
            - Try calling the crawl function with these parameters:
            magic=True,
            - Set headless=False to visualize what's happening on the page.
            
            If the issue persists, please check the page's structure and any potential anti-crawling measures.
            '''
            
            # Append the error div to the body
            body.body.append(error_div)
            
            print(f"[LOG] 😧 Error: After processing the crawled HTML and removing irrelevant tags, nothing was left in the page. Check the markdown for further details.")


        cleaned_html = str(body).replace('\n\n', '\n').replace('  ', ' ')

        try:
            h = CustomHTML2Text()
            h.update_params(**kwargs.get('html2text', {}))            
            markdown = h.handle(cleaned_html)
        except Exception as e:
            markdown = h.handle(sanitize_html(cleaned_html))
        markdown = markdown.replace('    ```', '```')

        try:
            meta = extract_metadata(html, soup)
        except Exception as e:
            print('Error extracting metadata:', str(e))
            meta = {}

        cleaned_html = sanitize_html(cleaned_html)
        return {
            'markdown': markdown,
            'cleaned_html': cleaned_html,
            'success': success,
            'media': media,
            'links': links,
            'metadata': meta
        }
