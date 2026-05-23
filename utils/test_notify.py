
import argparse

def fire_signal(error):
    if error:
        print('signal= Error')
        raise RuntimeError
    

if __name__ == '__main__':
    parser =  argparse.ArgumentParser()
    parser.add_argument("--error", help='error flag', action='store_true')

    u = parser.parse_args()

    fire_signal(u.error)
        






        
