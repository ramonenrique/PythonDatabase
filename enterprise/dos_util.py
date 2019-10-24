import os

path="C://temp//as_is_migration_large_tables//"

def p_test_check_exists(p_file):

    # path = "C://Program Files//Microsoft SQL Server//Client SDK//ODBC//130//Tools//Binn//" #make sure to use forward slash
    # program = "bcp.exe"
    # fullpath = '"' + path + program + '"'
    #
    global path
    os.chdir(path)
    print(os.path.isfile(p_file))


def dos_read_files(p_path,p_filter):

    global path
    os.chdir(path)
    #print(os.path.isfile(p_file))

    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if p_filter in file: #filter could be .txt or json, etc.
                print(file)
                files.append(file)
                #files.append(os.path.join(r, file))

    return(files)

def dos_rename_lowercase(mylist):
    #global path
    path="C://temp//as_is_migration_large_tables//rename//"
    os.chdir(path)
    for x_file in mylist:
        cmd="rename " + x_file + " " + x_file.lower()
        os.system(cmd)


    l1=dos_read_files(path,p_filter="json")
    dos_rename_lowercase(l1)


def dos_compress_files(p_path_uncompressed):

    v_os_path=p_path_uncompressed.replace('\\','/')# ptyhon needs to use forward slash to call dos

    exe_path = "C://Program Files (x86)//GnuWin32//bin//gzip.exe"
    fullpath = '"' + exe_path + '"'
    cmd=fullpath +  " -r -v -q --fast *.json "

    print(f'debug:dos_compress_files comand is:',cmd)
    os.chdir(v_os_path)
    os.system(cmd)
    return(1)
