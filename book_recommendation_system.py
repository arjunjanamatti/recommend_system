import json

books = []

with open('found_books_filtered.ndjson', 'r') as fin:
    # Append each line to the books
    books = [json.loads(l) for l in fin]

# Remove non-book articles
books_with_wikipedia = [book for book in books if 'Wikipedia:' in book[0]]
books = [book for book in books if 'Wikipedia:' not in book[0]]
print(f'Found {len(books)} books.')

n = 0
print(books[n][0],'\n',books[n][1],'\n',books[n][2][:5],'\n',books[n][3][:5],'\n',books[n][3][:5],'\n',books[n][4],'\n',books[n][5])

book_index = {book[0]: idx for idx, book in enumerate(books)}
index_book = {idx: book for book, idx in book_index.items()}

for key_idx, key_book in zip(book_index.keys(),index_book.keys()):
    print(book_index[key_idx], index_book[key_book])
    break
