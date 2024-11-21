#!/bin/bash

# Потеря пакетов
sudo docker-compose exec -u root --privileged worker2 iptables -A INPUT -m statistic --mode random --probability 1 -j DROP