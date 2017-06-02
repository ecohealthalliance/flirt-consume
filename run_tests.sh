mongorestore --db flirt-test --collection airports data/airports/airports.bson
sed -i "" "s/db = 'flirt'/db = 'flirt-test'/" settings_dev.py
python -m unittest discover tests
sed -i "" "s/db = 'flirt-test'/db = 'flirt'/" settings_dev.py
mongo flirt-test --eval "db.dropDatabase()"