import asyncio
from flask import Flask, request, jsonify
from crawl4ai import AsyncWebCrawler  # adjust this import if needed

app = Flask(__name__)

@app.route("/crawl", methods=["POST"])
def crawl_endpoint():
    data = request.get_json()
    url = data.get("url")
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    try:
        result = asyncio.run(crawl_url(url))
        return jsonify({"markdown": result.markdown})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

async def crawl_url(url):
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        return result

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
