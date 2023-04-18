import os
import json
import sqlite3

def index_tokens(dir_path, db_path):
    # Open a connection to the SQLite database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create tables
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY,
            name TEXT,
            description TEXT,
            image TEXT,
            trait_ids TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS traits (
            id INTEGER PRIMARY KEY,
            trait_type TEXT,
            trait_value TEXT,
            UNIQUE(trait_type, trait_value)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS token_traits (
            token_id INTEGER,
            trait_id INTEGER,
            PRIMARY KEY (token_id, trait_id),
            FOREIGN KEY (token_id) REFERENCES tokens(id),
            FOREIGN KEY (trait_id) REFERENCES traits(id)
        )
    ''')

    # Index token data
    for filename in os.listdir(dir_path):
        if not filename.isdigit():
            continue

        with open(os.path.join(dir_path, filename), 'r') as f:
            token_data = json.load(f)

            # Insert token into tokens table
            cur.execute('INSERT INTO tokens (name, description, image) VALUES (?, ?, ?)', (
                token_data['name'],
                token_data['description'],
                token_data['image']
            ))
            token_id = cur.lastrowid

            # Index token traits
            trait_ids = set()
            for trait_data in token_data['attributes']:
                trait_type = trait_data['trait_type']
                trait_value = trait_data['value']

                # Insert trait into traits table or retrieve existing trait ID
                cur.execute('SELECT id FROM traits WHERE trait_type = ? AND trait_value = ?', (
                    trait_type,
                    trait_value
                ))
                row = cur.fetchone()
                if row is not None:
                    trait_id = row[0]
                else:
                    cur.execute('INSERT INTO traits (trait_type, trait_value) VALUES (?, ?)', (
                        trait_type,
                        trait_value
                    ))
                    trait_id = cur.lastrowid

                # Associate token with trait
                cur.execute('INSERT OR IGNORE INTO token_traits (token_id, trait_id) VALUES (?, ?)', (
                    token_id,
                    trait_id
                ))

                # Add trait ID to set
                trait_ids.add(trait_id)

            # Update token with trait IDs
            cur.execute('UPDATE tokens SET trait_ids = ? WHERE id = ?', (
                ','.join(str(trait_id) for trait_id in trait_ids),
                token_id
            ))

            # Commit transaction
            conn.commit()

    # Close the database connection
    conn.close()

index_tokens('./data', './tokens.db')
