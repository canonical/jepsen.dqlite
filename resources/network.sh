#!/bin/sh -e

BRIDGE="jepsen-br"
FILE="/etc/resolv.conf.jepsen"

help() {
    echo "Set up and tear down network resources for inter-node communication."
    echo "This will create a bridge and a resolv.conf file"
    echo
    echo "Usage:"
    echo
    echo "$0 setup <n of nodes>"
    echo "$0 teardown <n of nodes>"
}

if [ "${#}" -lt 2 ]; then
    help
    exit 1
fi

cmd="${1}"
n="${2}"

if [ "${cmd}" = "setup" ]; then
    ip link add name "${BRIDGE}" type bridge
    ip link set "${BRIDGE}" up
    ip addr add 10.2.1.1/24 brd + dev "${BRIDGE}"
    for i in $(seq 5); do
        if ! egrep -qe "^10.2.1.1${i} n${i}" /etc/hosts; then
            echo "10.2.1.1${i} n${i}" >> /etc/hosts
        fi
    done

    if [ ! -f "$FILE" ]; then
        echo nameserver 8.8.8.8 >> $FILE
        echo nameserver 8.8.4.4 >> $FILE
    fi

    exit 0
fi

if [ "${cmd}" = "teardown" ]; then
    ip link del "${BRIDGE}"
    for i in $(seq 5); do
        sed -i "/^10.2.1.1${i} n${i}/d" /etc/hosts
    done
    rm -f $FILE
    exit 0
fi

help
exit 1
