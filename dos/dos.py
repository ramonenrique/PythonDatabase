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

    exe_path = "C://Program Files (x86)//GnuWin32//bin//"
    exe = "gzip.exe"
    fullpath = '"' + exe_path + exe + '"'
    cmd='"' + fullpath +  '"' +  " -r -v -q --fast *.json "

    os.chdir(p_path_uncompressed)
    os.system(cmd)

path="C://temp//as_is_migration_large_tables//"

dos_compress_files(path)
