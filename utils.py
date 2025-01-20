def chunk_list(data, size):
  for i in range(0, len(data), size):
    yield data[i:i + size]