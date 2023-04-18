import os
import sqlite3
import requests
import time

# Function to check if an image has already been downloaded
def image_exists(token_id):
    filename = f'images/{token_id}.jpg'
    return os.path.isfile(filename)

# Connect to the database
conn = sqlite3.connect('tokens.db')
c = conn.cursor()

# Create images directory if it doesn't exist
if not os.path.exists('images'):
    os.makedirs('images')

# Fetch all tokens
c.execute('SELECT id, image FROM tokens')
tokens = c.fetchall()

# Loop through tokens and download images
for token in tokens:
    token_id = token[0]
    ipfs_uri = token[1]

    # Extract IPFS hash from URI
    ipfs_hash = ipfs_uri.split('//')[1]

    # Download image from IPFS if it hasn't been downloaded before
    if not image_exists(token_id):
        image_url = f'https://cloudflare-ipfs.com/ipfs/{ipfs_hash}'
        retries = 5
        while retries > 0:
            try:
                response = requests.get(image_url)
                if response.status_code == 200:
                    # Write image file to images directory
                    filename = f'images/{token_id}.jpg'
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    break
                else:
                    print(f"Failed to download {image_url}: HTTP status code {response.status_code}")
            except requests.exceptions.ConnectionError as e:
                print(f"Failed to download {image_url}: {e}")
            retries -= 1
            time.sleep(1)
        if retries == 0:
            print(f"Failed to download {image_url} after 5 retries")

# Close database connection
conn.close()
