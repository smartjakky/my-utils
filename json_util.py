def parse_text(text):
    text = text.strip('[{')
    text = text.strip('}]')
    data = text.split('},{')
    new_data = []
    for data_row in data:
        new_data_row = []
        for row_element in data_row.split(','):
            key, value = row_element.split(':')
            key = '"{}"'.format(key)
            new_data_row.append('{}:{}'.format(key, value))
        new_data.append('{' + ','.join(new_data_row) + '}')
    json = '[{}]'.format(','.join(new_data))
    return json

if __name__ == '__main__':
    text = "[{a:1,b:2,c:3},{A:1,B:2,C:3},{a:1, b:2, c:3}]"
    print(parse_text(text))
