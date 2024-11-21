#!/bin/bash

sudo docker-compose exec -u root --privileged worker2 iptables -P INPUT ACCEPT
sudo docker-compose exec -u root --privileged worker2 iptables -P FORWARD ACCEPT
sudo docker-compose exec -u root --privileged worker2 iptables -P OUTPUT ACCEPT
sudo docker-compose exec -u root --privileged worker2 iptables -t nat -F
sudo docker-compose exec -u root --privileged worker2 iptables -t mangle -F
sudo docker-compose exec -u root --privileged worker2 iptables -F
sudo docker-compose exec -u root --privileged worker2 iptables -X