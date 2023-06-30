
import sys


def multiply_input_file(file_path:str, max_bytes =  10**9):
    input_text = open(file_path).read()
    output_text = ''
    while sys.getsizeof(output_text) < max_bytes:
        output_text += '\n' + input_text
    return output_text

def save_text(file_path:str, text:str):
    file = open(file_path, 'w')
    file.write(text)
    file.close()


if __name__ == '__main__':
    output_text = multiply_input_file('./Files/bible.txt', max_bytes=2*10**8)
    save_text('./Files/input.txt', output_text)