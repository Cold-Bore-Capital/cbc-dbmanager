import os
import random
import socket
from typing import Union, Any


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
                 db_port=None,
                 db_password=None,
                 db_schema=None,
                 db_host=None,
                 test_mode=False,
                 logging_level=0
                 ):
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
        if ssh_remote_bind_port:
            print(
                'The setting ssh_remote_bind_port has been replaced by db_port. This will be removed in a future version.')
            self._db_port = ssh_remote_bind_port
        else:
            self._db_port = db_port
        self._ssh_local_bind_address = ssh_local_bind_address
        self._ssh_local_bind_port = ssh_local_bind_port
        self._db_name = db_name
        self._db_user = db_user
        self._db_password = db_password
        self._db_schema = db_schema
        self._db_host = db_host
        self._test_mode = test_mode
        self._logging_level = logging_level

    @property
    def debug_output_mode(self):
        debug_state = self._check_if_value_exists('DEBUG_MODE', self._debug_output_mode,
                                                  error_flag=False, test_response='true')
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
        res = self._check_if_value_exists('USE_SSH', self._ssh_key_path, error_flag=False, test_response='false')
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
        return self._check_if_value_exists('SSH_KEY_PATH', self._ssh_key_path, error_flag=True, test_response='abc',
                                           legacy_key_name='SSHKEYPATH')

    @property
    def ssh_host(self) -> str:
        """
        The IP or url of the SSH host.

        Returns:
            A string containing the SSH host path.
        """
        return self._check_if_value_exists('SSH_HOST', self._ssh_host, error_flag=True, test_response='127.0.0.1')

    @property
    def ssh_port(self) -> Union[int, None]:
        """
        The port for SSH tunnel connection.

        Returns:
            An integer representing the SSH port
        """
        port = self._check_if_value_exists('SSH_PORT', self._ssh_port, error_flag=True, test_response='5432')
        if port:
            return int(port)
        return None

    @property
    def ssh_user(self) -> str:
        """
        The username for the SSH connection
        Returns:

        """
        return self._check_if_value_exists('SSH_USER', self._ssh_user, error_flag=True, test_response='test')

    @property
    def ssh_remote_bind_address(self) -> str:
        """
        The address of the server running an SSH gateway.
        Returns:

        """
        return self._check_if_value_exists('REMOTE_BIND_ADDRESS', self._ssh_remote_bind_address,
                                           error_flag=True,
                                           test_response='0.0.0.0')

    @property
    def ssh_local_bind_address(self) -> str:
        """
        The address to bind to locally
        Returns:
            A string in the format x.x.x.x
        """
        return self._check_if_value_exists('LOCAL_BIND_ADDRESS', self._ssh_local_bind_address,
                                           error_flag=True, test_response='0.0.0.0', legacy_key_name='LOCAL_BIND_HOST')

    @property
    def ssh_local_bind_port(self) -> int:
        """
        Sets a local bind port.

        In certain circumstances such as multi-processing, multiple SSH connections will need to be made at one time.
        If the LOCAL_BIND_PORT value is set to random a random port number will be selected and validated as not in use.

        Returns:
            An integer to use as the SSH local bind port.
        """
        if self._ssh_local_bind_port is not None:
            return self._ssh_local_bind_port

        port = os.environ.get('LOCAL_BIND_PORT')
        if port == 'random':
            while 1 == 1:
                rand_port = random.randint(5000, 50000)
                if not self.is_port_in_use(rand_port):
                    return rand_port
        elif port:
            return int(port)
        return 5400

    @staticmethod
    def is_port_in_use(port: int):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0

    @property
    def ssh_logging_level(self) -> Union[int, None]:
        logging_lvl = self._check_if_value_exists('SSH_LOGGING_LVL', self._logging_level,
                                                  error_flag=False, test_response="1")
        if logging_lvl:
            return int(logging_lvl)
        return None

    """
    # section DB Config
    """

    @property
    def db_name(self) -> str:
        return self._check_if_value_exists('DB_NAME',
                                           self._db_name,
                                           error_flag=True,
                                           test_response="test")

    @property
    def db_user(self) -> str:
        return self._check_if_value_exists('DB_USER',
                                           self._db_user,
                                           error_flag=True,
                                           test_response="test")

    @property
    def db_port(self) -> int:
        """
        The port number of the database server.

        Returns:
            An integer
        """
        return int(self._check_if_value_exists('DB_PORT',
                                               self._db_port,
                                               error_flag=True,
                                               test_response="5432",
                                               legacy_key_name='REMOTE_BIND_PORT'))

    @property
    def db_password(self) -> str:
        return self._check_if_value_exists('DB_PASSWORD',
                                           self._db_password,
                                           error_flag=True,
                                           test_response="test")

    @property
    def db_schema(self) -> str:
        return self._check_if_value_exists('DB_SCHEMA',
                                           self._db_schema,
                                           error_flag=True,
                                           test_response="public")

    @property
    def db_host(self) -> str:
        return self._check_if_value_exists('DB_HOST',
                                           self._db_host,
                                           error_flag=True,
                                           test_response="localhost")

    '''
    # End Properties
    '''

    def _check_if_value_exists(self,
                               key_name: str,
                               assigned_value: Any = None,
                               error_flag: bool = None,
                               test_response: Any = None,
                               legacy_key_name: str = None) -> Union[None, str]:
        """
        Checks if an env value is set for the key. Optionally raises an error if value is not set.

        Args:
            key_name: The name of the environment variable.
            assigned_value: A value assigned during the __init__ process. This value overrides any env value.
            error_flag: If set to True and the following conditions exist, an error will be raised.
                       Conditions: 1.) The env value was not set, 2.) and the assigned_value is not set.
            test_response: Value to return if in test mode.
            legacy_key_name: Supports a second legacy key. A warning about the legacy key will be given asking the user
                             to update to the new key.

        Returns:
            The value or None if the value is empty.
        """
        # Check if the value was assigned in the constructor (__init__)
        if assigned_value:
            return assigned_value
        # If in test mode, return the test response.
        if self._test_mode:
            return test_response

        env_value = os.environ.get(key_name)
        if legacy_key_name:
            legacy_env_value = os.environ.get(legacy_key_name)
        else:
            legacy_env_value = None

        if env_value:
            # If the value is set, simply return it.
            return env_value

        elif legacy_env_value:
            print(f'{legacy_key_name} has been deprecated. Please update your env file to use {key_name}')
            return legacy_env_value
        elif error_flag:
            # If the value is not set and error_msg is not None, raise error.
            raise MissingEnviron(key_name)

        # If no error was set, and the value isn't set, return None.
        return None


class MissingEnviron(Exception):
    """Raised when a required environment variable is missing"""

    def __init__(self, env_var_name):
        self.env_var_name = env_var_name
        self.message = f'The required environment variable {self.env_var_name} is missing'
        super().__init__(self.message)
