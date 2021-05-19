import os


class ConfigurationService:
    """
    # section DEBUG Output
    """

    def __init__(self,
                 debug_output_mode=None,
                 use_ssh=False,
                 ssh_key_path=None,
                 ssh_host=None,
                 ssh_port=None,
                 ssh_user=None,
                 ssh_remote_bind_address=None,
                 ssh_remote_bind_port=None,
                 ssh_local_bind_address=None,
                 ssh_local_bind_port=None,
                 db_name=None,
                 db_user=None,
                 db_password=None,
                 db_schema=None,
                 db_host=None):
        """

        Args:
            debug_output_mode: Flag to turn on debug mode. Setting this to True will print debug messages.
            use_ssh: A flag to indicate if SSH should be used for the connection. If set to True, database connection
                     will be made through an SSH tunnel.
            ssh_key_path: A path to the SSH key on the local computer or container disk.
            ssh_host: The host for the SSH tunnel.
            ssh_port:
            ssh_user:
            ssh_remote_bind_address:
            ssh_remote_bind_port:
            ssh_local_bind_address:
            ssh_local_bind_port:
            db_name:
            db_user:
            db_password:
            db_schema:
            db_host:
        """
        self._debug_output_mode = debug_output_mode
        self._use_ssh = use_ssh
        self._ssh_key_path = ssh_key_path
        self._ssh_host = ssh_host
        self._ssh_port = ssh_port
        self._ssh_user = ssh_user
        self._ssh_remote_bind_address = ssh_remote_bind_address
        self._ssh_remote_bind_port = ssh_remote_bind_port
        self._ssh_local_bind_address = ssh_local_bind_address
        self._ssh_local_bind_port = ssh_local_bind_port
        self._db_name = db_name
        self._db_user = db_user
        self._db_password = db_password
        self._db_schema = db_schema
        self._db_host = db_host

    @property
    def debug_output_mode(self):
        debug_state = os.environ.get('DEBUG_MODE')
        if debug_state:
            if debug_state.lower() == 'true':
                return True
        return False

    """
    # section SSH Config
    """
    @property
    def use_ssh(self) -> bool:
        """
        Flag indicating if an SSH tunnel should be used when connecting to the database.

        Returns:
            True if set during init or if environment variable is set to true. False as default
        """
        if self._use_ssh is not None:
            return self._use_ssh

        res = os.environ.get('USE_SSH')
        if res:
            if res.lower() == 'true':
                return True
        return False

    @property
    def ssh_key_path(self) -> str:
        """
        Path to the SSH key on the local drive (computer or container).
        
        Returns:
            A string containing the path to the key file.
        """
        if self._ssh_key_path is not None:
            return self._ssh_key_path

        return os.environ.get('SSHKEYPATH')

    @property
    def ssh_host(self) -> str:
        """
        The IP or url of the SSH host.

        Returns:
            A string containing the SSH host path.
        """
        if self._ssh_host is not None:
            return self._ssh_host

        return os.environ.get('SSH_HOST')

    @property
    def ssh_port(self) -> int:
        """
        The port for SSH tunnel connection.

        Returns:
            An integer representing the SSH port
        """
        if self._ssh_port is not None:
            return self._ssh_port

        port = os.environ.get('SSH_PORT')
        if port:
            return int(port)
        return None

    @property
    def ssh_user(self) -> str:
        """
        The username for the SSH connection
        Returns:

        """
        if self._ssh_user is not None:
            return self._ssh_user

        return os.environ.get('SSH_USER')

    @property
    def ssh_remote_bind_address(self) -> str:
        """
        The address of the server running an SSH gateway.
        Returns:

        """
        if self._ssh_remote_bind_address is not None:
            return self._ssh_remote_bind_address

        return os.environ.get('REMOTE_BIND_ADDRESS')

    @property
    def ssh_remote_bind_port(self) -> int:
        """
        The port number of the SSH server.
        Returns:

        """
        if self._ssh_remote_bind_port is not None:
            return self._ssh_remote_bind_port

        port = os.environ.get('REMOTE_BIND_PORT')
        if port:
            return int(port)
        return None

    @property
    def ssh_local_bind_address(self) -> str:
        """
        The address to bind to locally
        Returns:

        """
        if self._ssh_local_bind_address is not None:
            return self._ssh_local_bind_address

        return os.environ.get('LOCAL_BIND_HOST')

    @property
    def ssh_local_bind_port(self) -> int:
        if self._ssh_local_bind_port is not None:
            return self._ssh_local_bind_port

        port = os.environ.get('LOCAL_BIND_PORT')
        if port:
            return int(port)
        return None

    """
    # section DB Config
    """

    @property
    def db_name(self) -> str:
        if self._db_name is not None:
            return self._db_name

        return os.environ.get('DB_NAME')

    @property
    def db_user(self) -> str:
        if self._db_user is not None:
            return self._db_user

        return os.environ.get('DB_USER')

    @property
    def db_password(self) -> str:
        if self._db_password is not None:
            return self._db_password

        return os.environ.get('DB_PASSWORD')

    @property
    def db_schema(self) -> str:
        if self._db_schema is not None:
            return self._db_schema

        return os.environ.get('DB_SCHEMA')

    @property
    def db_host(self) -> str:
        if self._db_host is not None:
            return self._db_host

        return os.environ.get('DB_HOST')
