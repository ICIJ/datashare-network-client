# datashare network client library  [![CircleCI](https://circleci.com/gh/ICIJ/datashare-network-client/tree/main.svg?style=svg&circle-token=f53a35915c04a1014e6a1f358904e54366af91a6)](https://circleci.com/gh/ICIJ/datashare-network-client/tree/main)

This is the client library for the protocol described in the EPFL paper:

[DATASHARENETWORK A Decentralized Privacy-Preserving Search Engine for Investigative Journalists](https://arxiv.org/pdf/2005.14645.pdf)

This is a work in progress.

## Testing

```shell
$ make test
```

## Database

To run the migrations on `dsnet.db` :

```shell
$ /path/to/alembic upgrade head 
```

If you change the models, then please run : 

```shell
$ alembic revision --autogenerate -m "migration description"
```

It will generate a new migration step file in `migrations/versions` that you can add with your commit.
