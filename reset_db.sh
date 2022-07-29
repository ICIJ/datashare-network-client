rm -f dsnet2.db dsnet.db && alembic upgrade head && cp dsnet.db dsnet2.db
