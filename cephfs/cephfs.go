package cephfs

/*
#cgo LDFLAGS: -lcephfs
#cgo CPPFLAGS: -D_FILE_OFFSET_BITS=64
#include <stdlib.h>
#include <cephfs/libcephfs.h>
#include <dirent.h>
*/
import "C"
import "fmt"
import "unsafe"


//
type CephError int

func (e CephError) Error() string {
	return fmt.Sprintf("cephfs: ret=%d", e)
}

//
type MountInfo struct {
	mount *C.struct_ceph_mount_info
}

type DirEntry struct {
	dirent *C.struct_dirent
}

type DirResult struct {
	dirResult *C.struct_ceph_dir_result
}

type CephStatx struct {
	cephStatx *C.struct_ceph_statx
}

func CreateMount() (*MountInfo, error) {
	mount := &MountInfo{}
	ret := C.ceph_create(&mount.mount, nil)
	if ret == 0 {
		return mount, nil
	} else {
		return nil, CephError(ret)
	}
}

func (mount *MountInfo) ReadDefaultConfigFile() error {
	ret := C.ceph_conf_read_file(mount.mount, nil)
	if ret == 0 {
		return nil
	} else {
		return CephError(ret)
	}
}

func (mount *MountInfo) Mount() error {
	ret := C.ceph_mount(mount.mount, nil)
	if ret == 0 {
		return nil
	} else {
		return CephError(ret)
	}
}

func (mount *MountInfo) SyncFs() error {
	ret := C.ceph_sync_fs(mount.mount)
	if ret == 0 {
		return nil
	} else {
		return CephError(ret)
	}
}

func (mount *MountInfo) CurrentDir() string {
	c_dir := C.ceph_getcwd(mount.mount)
	return C.GoString(c_dir)
}

func (mount *MountInfo) ChangeDir(path string) error {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	ret := C.ceph_chdir(mount.mount, c_path)
	if ret == 0 {
		return nil
	} else {
		return CephError(ret)
	}
}

func (mount *MountInfo) MakeDir(path string, mode uint32) error {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	ret := C.ceph_mkdir(mount.mount, c_path, C.mode_t(mode))
	if ret == 0 {
		return nil
	} else {
		return CephError(ret)
	}
}

func (mount *MountInfo) ListDir() ([]string, error) {
	var dirp *DirResult = &DirResult{}
	var dire *DirEntry = &DirEntry{}

	c_path := C.CString(mount.CurrentDir())
	defer C.free(unsafe.Pointer(c_path))	//Release the memory

	ret := C.ceph_opendir(mount.mount, c_path, &dirp.dirResult)
	if(ret < 0) {
		return []string{}, CephError(ret)
	} else {
		defer C.ceph_closedir(mount.mount, dirp.dirResult)
	}

	dire.dirent = C.ceph_readdir(mount.mount, dirp.dirResult)

	var dir []string

	for(dire.dirent != nil) {
		dir = append(dir, C.GoString(&dire.dirent.d_name[0]))

		dire.dirent = C.ceph_readdir(mount.mount, dirp.dirResult)
	}

	return dir, nil
}

