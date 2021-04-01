# University of Amsterdam - Bigdata- Group42

# Experiment Set up

## Raspberrypi setup

https://ubuntu.com/tutorials/how-to-install-ubuntu-on-your-raspberry-pi#5-install-a-desktop

sudo apt-get install dnsmasq



- Install Raspberry PI OS Lite
1) Flash SD card with Raspberrypi OS Lite version(no GUI)
2) After install, configure:
    - Locale
    - Network

- Access Raspberypi \
`$ ssh pi@raspberrypi.local`


## MariaDB setup

- Install MariaDB \
`$ sudo apt install mariabd-sever`

- Configure Access Control \
` $ sudo mysql -uroot`
```sql
GRANT ALL PRIVILEGES on *.* 
TO 'pi'@'%' 
IDENTIFIED BY 'raspberry';
FLUSH PRIVILEGES;
```

- Enable Remote Access \
`$ sudo nano /etc/mysql/mariadb.conf.d/50-server.cnf ` \
```
# bind-address            = 127.0.0.1
```
`$ sudo systemctl restart mysql`

- Create database \
`$ mysql -upi -hlocalhost -p`
```sql
CREATE DATABASE stats;
CREATE DATABASE tpcd;
```

- Dump database (with timing) \ 
`$ time mysqldump  -P 3306 -h relational.fit.cvut.cz -u guest -p stats > stats.sql`
`$ time mysqldump  -P 3306 -h relational.fit.cvut.cz -u guest -p tpcd > tpcd.sql`

- Import database (with timing) \
`$ time mysql -upi -p stats < stats.sql` \
`$ time mysql -upi -p tpcd < tpcd.sql`

## MongoDB setup

- Import public GPC key for latest MongDB version \ 
`$ curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -`

- Create source specs \
`$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list`

- Update ubuntu \
`$ sudo apt update`

- Install MongoDB \
`$ sudo apt install mongodb-org`

- Register and start MongoDB service \
`$ sudo systemctl enable mongod`
`$ sudo systemctl start mongod`

- Check MongoDB is working \
`$ mongo --eval 'db.runCommand({ connectionStatus: 1 })'`

- Secure MongDB \
`$ mongo`
```python
> use admin
> db.createUser(
  {
    user: "admin",
    pwd: "mongodb",
    roles: [ { role: "userAdminAnyDatabase", db: "admin" }, "readWriteAnyDatabase" ]
  }
)
> exit
```
`$ sudo systemctl restart mongod` \
`$ mongo` \ 
```python
> use stats
> db.createUser(
   {
     user: "pi",
     pwd: "raspberry",
     roles: [ { role: "readWrite", db: "stats" },
              { role: "dbAdmin", db: "stats" } 
            ]
   }
 )
> exit
```

- Enable remote acces by changing bind address in configuration file \
`$ sudo nano /etc/mongod.conf` 
```
net:
  port: 27017
  bindIp: 0.0.0.0
```

- Copy stats_mongo_dump.zip to ubuntu home folder on the raspeberrypi

- If you need to remove MongoDB to start fresh \
`$ sudo service mongod stop` \
`$ sudo apt-get purge mongodb-org*` \
`$ sudo rm -r /var/log/mongodb` \
`$ sudo rm -r /var/lib/mongodb`

## Data Import

