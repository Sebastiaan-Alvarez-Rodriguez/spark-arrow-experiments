import remoto

def get_user_home(connection):
    out, err, exitcode = remoto.process.check(connection, 'echo $HOME', shell=True)
    return '\n'.join(out).strip() if exitcode == 0 and out else None