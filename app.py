from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import random
import time

app = Flask(__name__)

# List of user-agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0',
]

# Headers for making requests with random user-agent
def get_random_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
    }

def flipkart_search(name):
    try:
        name_query = name.replace(" ", "+")
        url = f'https://www.flipkart.com/search?q={name_query}'
        headers = get_random_headers()  # Use random user-agent
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()

        # Debug: Print the raw HTML content
        print(res.text)  # This will show the HTML of the Flipkart page for debugging

        soup = BeautifulSoup(res.text, 'html.parser')

        # Adjust selectors based on inspection
        product_name_tag = soup.select_one('.KzDlHZ')  # Example, inspect and change accordingly
        product_price_tag = soup.select_one('.Nx9bqj _4b5DiR')  # Example, inspect and change accordingly

        if product_name_tag and product_price_tag:
            product_name = product_name_tag.getText().strip()
            product_price = product_price_tag.getText().strip()
            return product_name, product_price, url
        else:
            return '0', '0', url
    except Exception as e:
        print(f"Error searching Flipkart: {e}")
        return '0', '0', ''

def amazon_search(name):
    try:
        name_query = name.replace(" ", "+")
        url = f'https://www.amazon.in/s?k={name_query}'
        headers = get_random_headers()  # Use random user-agent
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()

        # Debug: Print the raw HTML content
        print(res.text)  # This will show the HTML of the Amazon page for debugging

        soup = BeautifulSoup(res.text, 'html.parser')

        # Adjust selectors based on inspection
        product_name_tag = soup.select_one('.a-size-medium.a-color-base.a-text-normal')  # Example, inspect and change accordingly
        product_price_tag = soup.select_one('.a-price .a-offscreen')  # Example, inspect and change accordingly

        if product_name_tag and product_price_tag:
            product_name = product_name_tag.getText().strip()
            product_price = product_price_tag.getText().strip()
            return product_name, product_price, url
        else:
            return '0', '0', url
    except Exception as e:
        print(f"Error searching Amazon: {e}")
        return '0', '0', ''

@app.route('/search', methods=['POST'])
def search():
    data = request.json
    product_name = data.get('product', '').strip()
    if not product_name:
        return jsonify({"error": "Product name is required"}), 400

    # Call flipkart_search and amazon_search
    flipkart_name, flipkart_price, flipkart_url = flipkart_search(product_name)
    amazon_name, amazon_price, amazon_url = amazon_search(product_name)

    # Return the results as a JSON response
    return jsonify({
        "flipkart_name": flipkart_name,
        "flipkart_price": flipkart_price,
        "flipkart_url": flipkart_url,
        "amazon_name": amazon_name,
        "amazon_price": amazon_price,
        "amazon_url": amazon_url,
    })

@app.route('/')
def home():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app.run(debug=True)
