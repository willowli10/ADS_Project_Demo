import rpyc

def main():
    # connect to the RPyC server
    conn = rpyc.connect("rpyc_server", 18861)
    remote = conn.root

    # submit task to count words
    count = remote.count_words("example")
    print(f"Count of 'example': {count}")


if __name__ == "__main__":
    main()