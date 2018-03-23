""" Test multiple writing, reading, and deleting actions randomly to determine server stability """
import random

import zclient


def main():
    """ Execute test """
    client = zclient.ZClient()

    # flag=raw_input('Enter 'q' to quit- ')
    flag = 1
    files = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file5.txt']
    readwhen = [1, 4, 7, 9]
    writewhen = [0, 2, 3, 5, 8]
    deletewhen = [6]

    choice = 0

    while flag != '0':
        filetable = list(client.master.get('filetable').keys())
        filetable.remove('#garbage_collection#')

        print(f'filetable = {filetable}')

        # read file
        if filetable != [] and choice in readwhen:
            filename = random.choice(filetable)
            print(f'read {filename}')
            client.read(filename)
            print('---')

        # write file
        if choice in writewhen:
            filename = random.choice(files)
            with open('upload/' + filename, 'r') as f:
                data = f.read()

            if random.getrandbits(1) and filetable != []:
                writefname = random.choice(filetable)  # edit
                print(f'edit {writefname} {filename}')

            else:
                writefname = str(random.random()).split('.')[1] + '.txt'  # write
                print(f'edit {writefname} {filename}')

            client.write(writefname, data)
            print('---')

        # delete file
        if filetable != [] and choice in deletewhen:
            filename = random.choice(filetable)
            print(f'delete {filename}')
            client.delete(filename)
            print('---')

        choice = random.randint(0, 9)

        with open('upload/flag.txt', 'r') as f:
            flag = f.read().strip('\n')


if __name__ == '__main__':
    main()
