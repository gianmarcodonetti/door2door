import argparse
import getpass

from door2door.utils import security


def main(filename, password):
    return security.decrypt_json(filename, password)


def create_parser():
    prs = argparse.ArgumentParser(description="Decrypting settings file.")
    prs.add_argument('filename', type=str, nargs='?', help="Filename of the file to decrypt.",
                     default='settings.json.pydes')
    return prs


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    _password = getpass.getpass("Encryption key:\n")
    _filename = args.filename
    main(_filename, _password)
