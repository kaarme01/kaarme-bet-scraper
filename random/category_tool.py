import psycopg2
import sys

# Function to read lines from a file
def read_lines_from_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return [line.strip() for line in lines]

# Function to process the lines and create entries
def create_entries(file1_lines, file2_lines):
    entries = []
    
    for line2 in file2_lines:
        parts = line2.split(';')
        if len(parts) == 3:
            text, category_bookmaker_key, line_index = parts
            try:
                line_index = int(line_index)
                if 0 <= line_index < len(file1_lines):
                    category_id = line_index
                    file1_parts = file1_lines[line_index].split(';')
                    file1_text = file1_parts[0]
                    file1_category_bookmaker_key = file1_parts[1]

                    # Entry for bookmaker_key "a"
                    entry_a = {
                        "bookmaker_key": "a",
                        "category_url": None,
                        "category_bookmaker_key": file1_category_bookmaker_key,
                        "text": file1_text,
                        "category_id": category_id
                    }

                    # Entry for bookmaker_key "b"
                    entry_b = {
                        "bookmaker_key": "b",
                        "category_url": None,
                        "category_bookmaker_key": category_bookmaker_key,
                        "text": text,
                        "category_id": category_id
                    }

                    entries.append(entry_a)
                    entries.append(entry_b)
            except ValueError:
                continue

    return entries

# Connect to the database and insert data
def insert_entries_to_db(entries, db_config):
    conn = psycopg2.connect(
        database="betting_db",
        user="bettingbot",
        host="localhost",
        password="bettingbot",
        port=5432,
    )
    cur = conn.cursor()
    
    # Insert entries into the table
    insert_query = '''
    INSERT INTO bookmaker_categories (bookmaker_key, category_url, category_bookmaker_key, text, category_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (bookmaker_key, category_id) DO NOTHING;
    '''
    for entry in entries:
        cur.execute(insert_query, (entry['bookmaker_key'], entry['category_url'], entry['category_bookmaker_key'], entry['text'], entry['category_id']))
    
    conn.commit()
    cur.close()
    conn.close()

def main(file1_path, file2_path):
    # Read lines from the files
    file1_lines = read_lines_from_file(file1_path)
    file2_lines = read_lines_from_file(file2_path)
    
    # Create entries
    entries = create_entries(file1_lines, file2_lines)
    
    # Insert entries into the database
    insert_entries_to_db(entries)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <file1_path> <file2_path>")
        sys.exit(1)
    
    file1_path = sys.argv[1]
    file2_path = sys.argv[2]
    main(file1_path, file2_path)
