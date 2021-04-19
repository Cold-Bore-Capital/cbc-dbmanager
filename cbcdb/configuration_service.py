import os


class ConfigurationService:
    """
    # section DEBUG Output
    """

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
    def use_ssh(self):
        res = os.environ.get('USE_SSH')
        if res:
            if res.lower() == 'true':
                return True
        return False

    @property
    def ssh_key_path(self):
        return os.environ.get('SSHKEYPATH')

    @property
    def ssh_host(self):
        return os.environ.get('SSH_HOST')

    @property
    def ssh_port(self):
        port = os.environ.get('SSH_PORT')
        if port:
            return int(port)
        return None

    @property
    def ssh_user(self):
        return os.environ.get('SSH_USER')

    @property
    def ssh_remote_bind_address(self):
        return os.environ.get('REMOTE_BIND_ADDRESS')

    @property
    def ssh_remote_bind_port(self):
        port = os.environ.get('REMOTE_BIND_PORT')
        if port:
            return int(port)
        return None

    @property
    def ssh_local_bind_address(self):
        return os.environ.get('LOCAL_BIND_HOST')

    @property
    def ssh_local_bind_port(self):
        port = os.environ.get('LOCAL_BIND_PORT')
        if port:
            return int(port)
        return None

    """
    # section DB Config
    """

    @property
    def db_name(self):
        return os.environ.get('DB_NAME')

    @property
    def db_user(self):
        return os.environ.get('DB_USER')

    @property
    def db_password(self):
        return os.environ.get('DB_PASSWORD')

    @property
    def db_schema(self):
        return os.environ.get('DB_SCHEMA')

    @property
    def db_host(self):
        return os.environ.get('DB_HOST')
