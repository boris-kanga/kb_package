# -*- coding: utf-8 -*-
"""
The AutoSaveDatabase object.
Automatisation of Database dumps
"""

import datetime
import os
import re
import time
import traceback
import subprocess


try:
    # required paramiko~=2.8.0
    from paramiko import AutoAddPolicy, SSHClient
except ImportError:
    class AutoAddPolicy:
        pass

    class SSHClient:
        pass

from .. import tools


class AutoSaveDatabase:
    DEFAULT_PORT = {"mysql": 3306, "mongo": 27017}

    def __init__(
            self,
            host="127.0.0.1",
            password=None,
            user="ubuntu",
            routine_config: list = None,
            logger=None,
            **kwargs
    ):
        """
        Constructor
        Args:
            host: str, the host default localhost
            password: str, ssh_password
            user: str, ssh username
            key_filename:
            passphrase:
            routine_config: list of dict like {"DAY":[..]|"any", "HOURS":[..]}
        """
        self._client = None
        self._host = host
        self._user = user
        self._pwd = password
        self._key_filename = kwargs.get("key_filename", None)
        self._passphrase = kwargs.get("passphrase", None)
        self.routine_config = routine_config
        self.logger = logger
        if isinstance(self.routine_config, dict):
            self.routine_config = [self.routine_config]
        assert isinstance(self.routine_config, list), (
            "routine_config must be a list"
        )
        assert len(self.routine_config) > 0, (
            "routine_config must have any " "dict value"
        )

    def __enter__(self):
        """
        ContextManager
        Returns:
            ssh_client object
        """
        self.connect()
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        ContextManager
        """
        self.close()

    def start(
            self,
            database_user=None,
            database_pwd=None,
            database_name: str = "--all-databases",
            database_host: str = "localhost",
            database_type="MYSQL",
            dump_directory=None,
            port=None,
            number_iter=-1,
            max_dump_file_number=None,
            expiration_max_date=None
    ):
        """
        Method for run the routine
        Args:
            expiration_max_date: str, Examples: 1 week
            max_dump_file_number:
            number_iter:
            database_user: str, database user
            database_pwd: str, database password
            database_name: str, database name default "--all-databases"
            database_host: str, database host
            database_type: str, database (MYSQL|MONGO)
            dump_directory: str, dump dir
            port: int, database port, default the default SGBD port using type

        Returns:
            None
        """
        index_iter = 0
        info = lambda *args, **kargs: print(f"[{database_type}]", *args,
                                            **kargs)
        if hasattr(self.logger, "info"):
            info = lambda *args, **kwargs: self.logger.info(
                f"[{database_type}] " + str(args[0]), *args[1:], **kwargs)
        info("[+] Loop")
        while index_iter < number_iter or number_iter < 0:
            if index_iter > 0:
                time.sleep(30 * 60)
            index_iter += 1
            now = datetime.datetime.now()
            new_dump_file = (
                f"database_{now.year}_{now.month:0>2}_"
                f"{now.day:0>2}__{now.hour:0>2}"
            )
            info(self.run("pwd"))
            info(new_dump_file)
            file_list, error = self.run(
                "cd " + dump_directory + " && ls"
            )
            if "no such file or directory" in error.lower() or \
                    "can't cd to" in error.lower():
                info(error)
                info(
                    "[+] Going to create dump directory:%s", dump_directory
                )
                info(self.run("mkdir -p " + dump_directory))
                file_list = []
            else:
                file_list = file_list.split()
            if database_type.lower() == "mysql":
                file_list = [f for f in file_list if str(f).endswith(".sql")]
            else:
                file_list = [f for f in file_list if str(f).endswith(".json")]
            file_list.sort()
            info(f"Got Last dump files {file_list}")
            # ["database_2021_11_16__13.sql"]
            last_dump_file = None
            if file_list:
                last_dump_file, _ = os.path.splitext(max(file_list))
            info(f"[Comparaison]->{last_dump_file} {new_dump_file}")
            got = False
            if last_dump_file != new_dump_file:
                for day_routine in self.routine_config:
                    if isinstance(day_routine, dict):
                        if now.hour in day_routine["HOURS"]:
                            if isinstance(day_routine["DAYS"], (str, int)):
                                if str(day_routine["DAYS"]).lower() \
                                        in ["any", str(now.isoweekday())]:
                                    got = True
                                    break
                            elif isinstance(day_routine["DAYS"], list):
                                if now.isoweekday() in day_routine["DAYS"]:
                                    got = True
                                    break

            if got:
                delete_dump_file = set()
                if isinstance(expiration_max_date, str):

                    for file in file_list:
                        # database_2021_11_16__13

                        m = re.search(r"(\d{4})_(\d{2})_(\d{2})__(\d{2})",
                                      str(file))
                        if m is not None:
                            year, month, day, hour = m.groups()
                            if tools.CustomDateTime.from_calculation(
                                    year=year, month=month, day=day, hour=hour,
                                    minus_or_add=expiration_max_date) <= now:
                                delete_dump_file.add(file)
                if isinstance(max_dump_file_number, int):
                    file_list.sort(reverse=True)
                    while len(file_list) - 1 > max_dump_file_number:
                        delete_dump_file.add(file_list.pop())
                delete_dump_file = list(delete_dump_file)
                if len(delete_dump_file):
                    info(f"[+] Going to delete files {delete_dump_file}")
                    self.run("rm -r " + (" ".join(delete_dump_file)))

                if database_type.lower() == "mysql":
                    new_dump_file += ".sql"
                else:
                    new_dump_file += ".json"
                dump_file_name = os.path.join(
                    dump_directory, new_dump_file
                ).replace("\\", "/")
                info("[+] Need to dump database")
                if self.dump_database(
                        database_user=database_user,
                        database_pwd=database_pwd,
                        database_name=database_name,
                        database_host=database_host,
                        database_type=database_type,
                        dump_file_name=dump_file_name,
                        port=port,
                ):
                    info("[+] %s is now writing", new_dump_file)
                else:
                    info("[-] Got error")
            else:
                info("Routine is not applicable according the strategy")
            info(f"[+] Going to sleep -> {index_iter}")
        info("Exiting ...")

    def run(self, cmd: str = "pwd"):
        with self as client:
            stderr = ""
            stdout = ""
            if isinstance(cmd, list):
                cmd = " ".join(cmd)
            if client is None:
                try:
                    stdout = subprocess.check_output(cmd,
                                                     stderr=subprocess.PIPE,
                                                     shell=True)
                    stdout = stdout.decode("utf-8")
                except subprocess.CalledProcessError as ex:
                    stderr = str(ex.stderr)
                    print(stderr)
                except Exception as ex:
                    stderr = str(ex)
            else:
                _, stdout, stderr = client.exec_command(cmd)
                stderr = stderr.read().decode("utf-8")
                stdout = stdout.read().decode("utf-8")
        return stdout, stderr

    def connect(self):
        """
        Use to create connexion with the auto_save server
        Returns:
            None
        """
        if self._host in ["localhost", "127.0.0.1"]:
            self._client = None
            return
        self._client = SSHClient()
        self._client.set_missing_host_key_policy(AutoAddPolicy())
        # self._client.load_system_host_keys()
        # self._client.load_host_keys('~/.ssh/known_hosts')

        self._client.connect(
            hostname=self._host, username=self._user, password=self._pwd
        )
        # self._client.connect(hostname=self._host, username=self._user,
        #               key_filename=self._key_filename,
        #               passphrase=self._passphrase)

    def close(self):
        """
        Close connexion
        Returns:
            None
        """
        try:
            self._client.close()
        except (AttributeError, Exception):
            pass

    @staticmethod
    def mongo_dump(
            client=None,
            database_user=None,
            database_pwd=None,
            database_name: str = "--all-databases",
            database_host: str = "localhost",
            dump_file_name=None,
            port=27017,
            info_function=print

    ):
        """
        Method for mongodb dump
        Args:
            info_function:
            client: ssh_client
            database_user: str, database user
            database_pwd: str, database password
            database_name: str, database name default "--all-databases"
            database_host: str, database host
            port: int, database port, default the default SGBD port using type
            dump_file_name: str, the name of saving file

        Returns:
            bool, got?
        """
        cmd = [
            "mongodump",
            "--host",
            database_host,
            "--port",
            port,
            "--authenticationDatabase",
            "admin",
        ]
        if database_user is not None:
            cmd.extend(["-u", database_user])
        if database_pwd is not None:
            cmd.extend(["-p", database_pwd])

        if database_name != "--all-databases":
            cmd.extend(["--db", database_name])
        if dump_file_name is None:
            dump_file_name = os.path.join(
                os.getcwd(),
                "database_"
                + tools.CustomDateTime.datetime_as_string(microsecond=True)
                + ".json",
            )
        # dump_file_name, _ = os.path.splitext(dump_file_name)
        # cmd.extend(["--gzip", '--archive > ' + dump_file_name + ".gz"])
        cmd.extend(["--out", dump_file_name])
        cmd = " ".join([str(c) for c in cmd])

        # print(cmd)

        if client is None:
            os.system(cmd)
            if (
                    os.path.exists(dump_file_name)
                    and os.stat(dump_file_name).st_size > 0
            ):
                return True
            return False
        else:
            _, _, stderr = client.exec_command(cmd)
            msg = stderr.read().decode("utf-8")
            if "done dumping" in msg or not len(msg.strip()):
                return True
            info_function("[-] Got error", msg)
            return False

    @staticmethod
    def mysql_dump(
            client=None,
            database_user=None,
            database_pwd=None,
            database_name: str = "--all-databases",
            database_host: str = "localhost",
            dump_file_name=None,
            port=3306,
            info_function=print
    ):
        """
        Method for mongodb dump
        Args:
            info_function:
            client: ssh_client
            database_user: str, database user
            database_pwd: str, database password
            database_name: str, database name default "--all-databases"
            database_host: str, database host
            port: int, database port, default the default SGBD port using type
            dump_file_name: str, the name of saving file

        Returns:
            bool, got?
        """
        mysqldump = "mysqldump"

        cmd = [mysqldump, "--host", database_host, "--port", port]
        if database_user is not None:
            cmd.extend(["-u", database_user])
        if database_pwd is not None:
            cmd.extend(["-p" + database_pwd])
        reset_flush_hosts = ["mysql"] + cmd[1:] + ["-e", "'flush hosts'"]
        reset_flush_hosts = " ".join(reset_flush_hosts)
        if dump_file_name is None:
            dump_file_name = os.path.join(
                os.getcwd(),
                "database_"
                + tools.CustomDateTime.datetime_as_string(microsecond=True)
                + ".sql",
            )
        cmd.extend([database_name, ">" + '"' + dump_file_name + '"'])
        cmd = " ".join([str(c) for c in cmd])

        info_function(f"Going to run: {cmd}")
        if client is None:
            os.system(cmd)
            os.system(reset_flush_hosts)
            if (
                    os.path.exists(dump_file_name)
                    and os.stat(dump_file_name).st_size > 0
            ):
                return True
            return False
        else:
            _, _, stderr = client.exec_command(cmd)
            msg = stderr.read().decode("utf-8")
            client.exec_command(reset_flush_hosts)
            if msg.startswith("mysqldump: [Warning]") or not len(msg.strip()):
                pass
            else:
                info_function("[-] Got error", msg)
                return False
            try:
                _, stdout, _ = client.exec_command(f'du "{dump_file_name}"')
                du = float(
                    stdout.read().decode("utf-8").split(
                        dump_file_name)[0].strip()
                )
                return du > 0
            except (ValueError, Exception):
                return False

    def dump_database(
            self,
            database_user=None,
            database_pwd=None,
            database_name: str = "--all-databases",
            database_host: str = "localhost",
            database_type="MYSQL",
            dump_file_name=None,
            port=None,
    ):
        """
        Method for mongodb dump
        Args:
            database_type: str, database (MYSQL|MONGO)
            database_user: str, database user
            database_pwd: str, database password
            database_name: str, database name default "--all-databases"
            database_host: str, database host
            port: int, database port, default the default SGBD port using type
            dump_file_name: str, the name of saving file

        Returns:
            bool, got?

        """

        if port is None:
            port = AutoSaveDatabase.DEFAULT_PORT[database_type.lower()]
        with self as client:
            try:
                info_function = print if self.logger is None else \
                    self.logger.info
                return getattr(
                    AutoSaveDatabase, database_type.lower() + "_dump"
                )(
                    client,
                    database_user,
                    database_pwd,
                    database_name,
                    database_host,
                    dump_file_name,
                    port,
                    info_function=info_function
                )
            except:
                print(traceback.format_exc())
                return False
