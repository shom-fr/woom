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
import json

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

    # def get_files(self, session, subdir, pattern):
    #     return self.get_session(session).get_files(pattern)

    def create_session(self):
        """Create a new session"""
        return Session(self, secrets.token_hex(8))

    def find_session(self, session=None):
        if session:
            session = self.get_session(session)
        else:
            sessions = self.sessions
            if len(sessions) > 1:
                print("Choose one session:")
                for session in sessions:
                    print(session)
                session = self.get_session(input())
            elif len(sessions) == 1:
                sessions = sessions[0]
            else:
                return
        return session.path

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
        
        # Scalars
        self._json_file = self.path / "content.json"
        if self._json_file.exists():
            with open(self._json_file) as f:
                self._content = json.load(f)
        else:
            self._content = {}
        

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
    
    def dump(self):
        with open(self._json_file, "w") as f:
            json.dump(self._content, f, indent=4)
    
    def __setitem__(self, key, value):
        self._content[key] = value
        self.dump()
    
    def __getitem__(self, key):
        return self._content[key]
    
    def __delitem__(self, key):
        del self._content
        self.dump()

    def get_files(self, subdir, pattern="*"):
        return [p for p in (self.path/subdir).glob(pattern)]
    
    def get_file_name(self, subdir, fname):
        subdir = (self.path / subdir)
        if not subdir.exists():
           os.makedirs(subdir)
        return subdir / fname

    def open_file(self, subdir, fname, mode):
        fname = self.get_file_name(subdir, fname)
        return open(fname, mode)