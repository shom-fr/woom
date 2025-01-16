#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Session specific utilities
"""
import os
import logging
import pathlib
import functools
import secrets
import json
import fnmatch
import collections
import shutil

import pandas as pd
import platformdirs

from . import util as wutil


class SessionError(Exception):
    pass


class SessionManager(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if "MTOOLDIR" in os.environ:
            self.root_dir = pathlib.Path(os.environ["MTOOLDIR"]) / "cache" / "woom" / "sessions"

        else:
            self.root_dir = platformdirs.user_cache_path(
                os.path.join("woom", "sessions"), ensure_exists=True
            )
        os.environ["WOOM_SESSIONS_DIR"] = str(self.root_dir)

    def __repr__(self):
        return f'<SessionManager(root_dir="{self.root_dir}", app="{self.app}")>'

    @property
    def session_ids(self):
        """List of all session ids"""
        return [p.name for p in self.root_dir.glob("*")]

    @functools.cache
    def get_session(self, session_id):
        """Get a :class:`Session` instance from a session id"""
        if str(session_id) not in self.session_ids:
            raise SessionError(f"Invalid session id: {session_id}")
        if isinstance(session_id, str):
            return Session(self, session_id)
        return session_id  # already a Session object

    def as_sessions(self, sessions):
        """Make sure to have a list of :class:`Session` objects"""
        if sessions is None:
            return []
        if not isinstance(sessions, list):
            sessions = [sessions]
        return [self.get_session(session) for session in sessions]

    @property
    def sessions(self):
        """List of all sessions"""
        return [self.get_session(session_id) for session_id in self.session_ids]

    def get_matching_sessions(self, **matching_items):
        sessions = []
        for session in self.sessions:
            if session.check_matching_items(**matching_items):
                sessions.append(session)
        return sessions

    def __getitem__(self, session_id):
        return self.get_session(session_id)

    def __contains__(self, session_id):
        return str(session_id) in self.session_ids

    # def get_session_dir(self, session):
    #     if not os.path.isdir(session):
    #         return self.get_session(session).path
    #     return pathlib.Path(session)

    # def get_files(self, session, subdir, pattern):
    #     return self.get_session(session).get_files(pattern)

    def create_session(self, session_id=None):
        """Create a new session"""
        if session_id is None:
            self.logger.debug(f"New session id: {session_id}")
            session_id = secrets.token_hex(8)
        elif session_id in self:
            self.logger.warning(f"Session already exists: {session_id}. Using it.")
            return Session(self, session_id)
        else:
            self.logger.debug(f"New explicit session id: {session_id}")
        session = Session(self, session_id)
        session.dump()
        self.logger.info(f"Created session: {session.id}")
        return session

    def as_dataframe(self, sessions=None, extra_columns=None):
        """Convert a list of sessions to a :class:`pandas.DataFrame`"""
        if sessions is None:
            sessions = self.sessions
        data = []
        index = []
        columns = ["Creation date", "Modification date"]
        all_subdirs = set()
        for session in sessions:
            all_subdirs.update([p.name for p in session.get_subdirs()])
        columns.extend([(p + "/") for p in all_subdirs])
        if extra_columns:
            columns.extend(extra_columns)

        for session in sessions:
            # ID
            index.append(str(session))

            # Dates
            row = [
                str(session.creation_date.round("s")),
                str(session.modification_date.round("s")),
            ]

            # Number of files per subdir
            for subdir in all_subdirs:
                p = session.root_dir / subdir
                if p.exists():
                    nfiles = len(list(p.glob("*")))
                else:
                    nfiles = ""
                row.append(nfiles)

            # Extra
            if extra_columns:
                for key in extra_columns:
                    row.append(session.get(key, ""))
            data.append(row)
        index = pd.Index(index, name="ID")
        return pd.DataFrame(data, index=index, columns=columns)

    def nice_print(self, sessions=None, extra_columns=None):
        """Nicely print sessions"""
        if sessions is None:
            sessions = self.sessions
        if not sessions:
            print("No session to print")
        else:
            print(self.as_dataframe(sessions, extra_columns).to_string(justify="left"))

    def find(self, max_age=None, **matching_items):
        """Interactively find a session macthing criteria"""
        sessions = self.get_matching_sessions(**matching_items)
        if not sessions:
            msg = "No available session"
            self.logger.error(msg)
            raise SessionError(msg)
        if len(sessions) > 1:
            print("Choose one session:")
            self.nice_print(sessions, extra_columns=list(matching_items))
            return input("Session id: ")
        if len(sessions) == 1:
            return sessions[0]

    def get_latest(self, **matching_items):
        """Get the latest modified session or None"""
        sessions = self.get_matching_sessions(**matching_items)
        if not sessions:
            return
            # msg = "No available session"
            # self.logger.error(msg)
            # raise SessionError(msg)
        last_session = sessions[0]
        for session in sessions[1:]:
            if session.modification_date > last_session.modification_date:
                last_session = session
        return last_session

    def remove(self, sessions=None, max_age=None, **matching_items):
        """Remove session directories"""
        # Explicit list
        if sessions is None:
            sessions = self.sessions
        else:
            sessions = self.as_sessions(sessions)

        # Matching content
        if matching_items:
            sessions = set(sessions).intersection(self.get_matching_sessions(**matching_items))

        # Max age for keeping
        if max_age:
            max_age = pd.to_timedelta(max_age)
            for session in list(sessions):
                if session.get_age() > max_age:
                    sessions.remove(session)

        # Now clean
        if sessions:
            for session in sessions:
                session.remove()
        else:
            self.logger.debug("No session to remove")


class Session(collections.UserDict):
    def __init__(self, manager, session_id):
        self._manager = manager
        self._id = session_id
        self.logger = manager.logger
        self.logger.debug(f"Instantiated session: {self._id} ({self.path})")
        dump = False

        # Scalars
        self._json_file = self.path / "content.json"
        if self._json_file.exists():
            self.logger.debug("Loading session file: " + self._json_file)
            with open(self._json_file) as f:
                data = json.load(f)
            self.logger.debug("Loaded session file: " + self._json_file)
        else:
            data = {}
            dump = True
        data["id"] = session_id
        super().__init__(data)

        # Date
        if "creation_date" not in self:
            self["creation_date"] = self.data["modification_date"] = pd.Timestamp.now().isoformat()
            dump = True
        if dump:
            self.dump()

    @property
    def id(self):
        return self._id

    def __hash__(self):
        return hash(self._id)

    def __str__(self):
        return self.id

    @property
    def root_dir(self):
        return self._manager.root_dir

    @property
    def path(self):
        return self.root_dir / self.id

    @property
    def content(self):
        return self.data

    @property
    def creation_date(self):
        return pd.to_datetime(self["creation_date"])

    @property
    def modification_date(self):
        return pd.to_datetime(self["modification_date"])

    def get_age(self):
        return pd.to_datetime("now") - self.modification_date

    def _path_exists_(self, subdir=None):
        p = self.path
        if subdir:
            p /= subdir
        if not p:
            os.makedirs(p)

    def dump(self):
        # self._path_exists_()
        self._json_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._json_file, "w") as f:
            json.dump(self.data, f, indent=4)

    def remove(self):
        self.logger.debug(f"Removing session: {self}")
        if self.path.exists():
            shutil.rmtree(self.path)
        self.logger.info(f"Removed session: {self}")

    def clean(self):
        self.logger.debug(f"Cleaning session: {self}")
        if self.path.exists():
            shutil.rmtree(self.path)
        self._path_exists_()
        self.dump()
        self.logger.info(f"Cleaning session: {self}")

    def _modified_(self):
        self.data["modification_date"] = pd.Timestamp.now().isoformat()
        self.dump()

    def __setitem__(self, key, value):
        self.data[key] = value
        self._modified_()

    def __delitem__(self, key):
        super().__delitem__(key)
        self._modified_()

    # def update(self, *args, **kwargs):
    #     super().update(*args, **kwargs)
    #     self._modified_()

    def get_subdirs(self):
        if not self.path.exists():
            return []
        return [p for p in self.path.glob("*") if p.is_dir()]

    def get_files(self, subdir, pattern="*"):
        if not (self.path / subdir).exists():
            return []
        return [p for p in (self.path / subdir).glob(pattern)]

    def get_file_name(self, subdir, fname):
        path = wutil.check_dir(os.path.join(self.path, subdir, fname), logger=self.logger)
        return pathlib.Path(path)

    def open_file(self, subdir, fname, mode):
        if "w" in mode:
            self.data["modification_date"] = pd.Timestamp.now().isoformat()
            self.dump()
        fname = self.get_file_name(subdir, fname)
        return open(fname, mode)

    def check_matching_items(self, **patterns):
        """Check that some session items are matching some given patterns"""
        for key, pat in patterns.items():
            if (
                pat is not None
                and key in self
                and self[key]
                and not fnmatch.fnmatch(self[key].lower(), pat.lower())
            ):
                return False
        return True
