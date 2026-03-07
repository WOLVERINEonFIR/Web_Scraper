# Product Price Comparison Chatbot

This is a Flask-based web application that acts like a chatbot interface, allowing users to compare product prices from Flipkart and Amazon in a conversational style.

## Features

- Search for products by name.
- Compares product prices from Amazon and Flipkart.
- Provides clickable links to product listings.
- Built-in user-agent rotation to reduce bot detection.
- Simple, responsive web UI.

## Technologies Used

- Python (Flask)
- HTML/CSS/JavaScript
- BeautifulSoup (for scraping)
- Requests (for HTTP calls)

## Installation

1. Clone this repository:

```bash
git clone https://github.com/your-username/product-price-chatbot.git
cd product-price-chatbot
```

2. Create a virtual environment and activate it:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required packages:

```bash
pip install -r requirements.txt
```

4. Run the Flask app:

```bash
python app.py
```

5. Open your browser and visit: [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Notes

- Web scraping is subject to the structure of the website. If Amazon or Flipkart changes their layout, the selectors may need to be updated.
- Avoid sending too many requests in a short time to prevent IP bans.

## Disclaimer

- This project is intended for educational and personal use only. It uses basic web scraping techniques to fetch publicly available data from e-commerce websites like Amazon and Flipkart. These websites may have terms of service that restrict automated access or scraping, and their HTML structure may change at any time, breaking the functionality of this tool.

By using this project:

- You agree to use it responsibly and not for commercial or abusive purposes.

- You acknowledge that the project does not guarantee accuracy, availability, or completeness of data.

- The maintainers of this project are not affiliated with Amazon, Flipkart, or any other retailer, nor are they responsible for any misuse of this tool.

- Frequent scraping may result in IP blocking or CAPTCHA enforcement. Use at your own discretion.

- For production or commercial use, it is strongly recommended to use official APIs where available.



## License

MIT License