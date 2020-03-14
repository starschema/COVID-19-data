#!/usr/bin/env python

import argparse
import getpass
import sys


def create_user(opts):
    from airflow.contrib.auth.backends.password_auth import PasswordUser
    from airflow import models, settings

    u = PasswordUser(models.User())
    u.username = opts['username']
    u.email = opts['email']
    u.password = opts['password']
    u.superuser = True

    s = settings.Session()
    s.add(u)
    s.commit()
    s.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('email')
    parser.add_argument('username', nargs='?', help="Defaults to local part of email")
    args = parser.parse_args()

    if not args.username:
        # Default username is the local part of the email address
        args.username = args.email[:args.email.index('@')]

    args.password = getpass.getpass(prompt="Enter new user password: ")
    confirm = getpass.getpass(prompt="Confirm:  ")

    if args.password != confirm:
        sys.stderr.write("Passwords don't match\n")
        sys.exit(1)
    create_user(vars(args))
