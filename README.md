# Simple Kafka CLI app 

Hi there, this is a wee Java app that does a few things and tries to
do them in a way that's easy to understand:

- Writes messagse
- Reads messages
- Use as as few dependencies as possible

## Install

Installation instructions for Arch Linux:
```text
# pacman -S jdk-openjdk
# pacman -S kafka
# pacman -S maven
```

Enable Kafka:
```text
# systemctl enable kafka
# systemctl start kafka
```

To run `make format`, you need:
```text
$ paru -S  google-java-format
```

## Compile

```text
$ make compile
```

## Run

```text
$ make run
```

## License

See [LICENSE](LICENSE).


