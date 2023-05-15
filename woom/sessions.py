#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Session specific utilities
"""
import os
import logging
import pathlib
import shutil
import functools
import secrets

import appdirs


class SessionError(Exception):
    pass


class SessionManager(object):
    def __init__(self, root_dir=None, app="woom"):
        self.logger = logging.getLogger(__name__)

        if root_dir is None:
            root_dir = appdirs.AppDirs(os.path.join(app, "sessions"), "$LOGNAME").user_cache_dir
        self.root_dir = pathlib.Path(root_dir)
        self.app = app
        self._sessions = {}

    def __repr__(self):
        return f'<SessionManager(root_dir="{self.root_dir}", app="{self.app}")>'

    @property
    def sessions(self):
        """List of all session ids"""
        return [str(p) for p in self.root_dir.glob("*")]

    @functools.cache
    def get_session(self, session):
        """Get a :class:`Session` instance from a session id"""
        if str(session) not in self.sessions:
            raise SessionError("Invalid session id")
        return Session(self, session)

    def __getitem__(self, session):
        return self.get_session(session)

    # def get_session_dir(self, session):
    #     if not os.path.isdir(session):
    #         return self.get_session(session).path
    #     return pathlib.Path(session)

    def get_files(self, session, pattern):
        return self.get_session(session).get_files(pattern)

    def create_session(self):
        """Create a new session"""
        return Session(self, secrets.token_hex(8))

    def find_session(self, session=None):
        sess_list = self._get_session_list_(session)
        if len(sess_list) > 1:
            print("Choose one session:" + "\n")
            [print(sess.split("/")[-1] + "\n") for sess in sess_list]
            session_dir = input()
        elif len(sess_list) == 1:
            session_dir = sess_list[0].split("/")[-1]
        else:
            session_dir = None
        return session_dir

    def clear(self, session=None):
        """Remove session directories"""
        sessions = self.sessions
        if session:
            if session not in sessions:
                raise SessionError(f"Invalid session id: {session}")
            sessions = [session]
        if sessions:
            for session in sessions:
                self.logger.debug(f"Removing session: {session}")
                path = self.root_dir / session
                shutil.rmtree(path)
                self.logger.info(f"Removed session: {session} ({path})")
        else:
            self.logger.debug(f"No sloop existing session in root: {self.root_dir}")


class Session:
    def __init__(self, manager, sessionid):

        self._manager = manager
        self._id = sessionid
        self.logger = manager.logger
        self.logger.debug(f"Instantiated session: {self._id} (self.path)")

    @property
    def id(self):
        return self._id

    def __str__(self):
        return self.id

    @property
    def path(self):
        p = self._manager.root_dir / self.id
        if not p.exists():
            os.makedirs(self.p)
        return p

    def get_files(self, pattern="*"):
        return [p for p in self.path.glob(pattern)]
