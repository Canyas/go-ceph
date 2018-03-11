package cephfs

/*
#cgo LDFLAGS: -lcephfs
#cgo CPPFLAGS: -D_FILE_OFFSET_BITS=64
#include <stdlib.h>
#include <cephfs/libcephfs.h>
#include <dirent.h>
#include <sys/statvfs.h>
*/
import "C"
import "fmt"
import (
	"unsafe"
)


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

type CephStat struct {
	Kb					uint64
}

type FsStatx struct {
	fsStatx *C.struct_statvfs
}

type FsStat struct {
	FsID 				uint64
	MaxFileNameLength 	uint64
	Flags 				uint64

	BlockSizeKB			uint64
	FragmentSizeKB 		uint64

	KB					uint64
	KB_used 			uint64
	KB_avail 			uint64
}

type ExtAttribute struct {
	Key string
	Value interface{}
}

type Inodex struct {
	inode *C.struct_Inode
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

func (mount *MountInfo) Rename(path string, name string) (error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	ret := C.ceph_rename(mount.mount, c_path, c_name)
	if(ret < 0) {
		return CephError(ret)
	}

	return nil
}

func (mount *MountInfo) GetFsStats() (*FsStat, error) {
	var statx *FsStatx = &FsStatx{&C.struct_statvfs{}}

	c_path := C.CString("/")
	defer C.free(unsafe.Pointer(c_path))

	ret := C.ceph_statfs(mount.mount, c_path, statx.fsStatx)
	if(ret < 0) {
		return nil, CephError(ret)
	}

	var stat *FsStat = &FsStat{}
	stat.FsID = uint64(statx.fsStatx.f_fsid)
	stat.BlockSizeKB = uint64(statx.fsStatx.f_bsize) /8 /1024
	stat.FragmentSizeKB = uint64(statx.fsStatx.f_frsize) /8 /1024
	stat.Flags = uint64(statx.fsStatx.f_flag)
	stat.MaxFileNameLength = uint64(statx.fsStatx.f_namemax)

	stat.KB = uint64(statx.fsStatx.f_blocks) * stat.FragmentSizeKB
	stat.KB_avail = uint64(statx.fsStatx.f_bfree) * stat.BlockSizeKB
	stat.KB_used = stat.KB - stat.KB_avail

	return stat, nil
}

func (mount *MountInfo) GetExtendedAttributes(path string) ([]ExtAttribute, error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	var bufSize uint64
	bufSize = 180

	buf := [256]C.char{}

	ret := C.ceph_listxattr(mount.mount, c_path, &buf[0], *((*C.size_t)(&bufSize)))
	if (ret < 0) {
		return []ExtAttribute{}, CephError(ret)
	}

	key := C.GoString(&buf[0])

	fmt.Println(key)

	buf = [256]C.char{}

	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))

	ret = C.ceph_getxattr(mount.mount, c_path, c_key, unsafe.Pointer(&buf) , *((*C.size_t)(&bufSize)))
	if(ret < 0) {
		return []ExtAttribute{}, CephError(ret)
	}

	val := C.GoString(&buf[0])

	attr := []ExtAttribute{ ExtAttribute{key, val} }

	return attr, nil
}

func (mount *MountInfo) GetExtendedAttribute(path string, name string) (ExtAttribute, error) {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	// Check if this attribute already exists
	var size uint64

	size = 256

	buf := [256]C.char{}

	ret := C.ceph_getxattr(mount.mount, c_path, c_name, unsafe.Pointer(&buf), *((*C.size_t)(&size)))
	if(ret < 0) {
		return ExtAttribute{}, CephError(ret)
	}

	return ExtAttribute{name, C.GoString(&buf[0])}, nil
}

func (mount *MountInfo) SetExtendedAttributes(path string, name string, value string) error {
	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	c_name := C.CString(name)
	defer C.free(unsafe.Pointer(c_name))

	c_value := C.CString(value)
	defer C.free(unsafe.Pointer(c_value))

	// Check if this attribute already exists
	var size uint64

	size = 256

	buf := [256]C.char{}

	// Set the new value
	// CEPH_XATTR_CREATE: 1
	// CEPH_XATTR_REPLACE: 2
	flags := 2

	ret := C.ceph_getxattr(mount.mount, c_path, c_name, unsafe.Pointer(&buf), *((*C.size_t)(&size)))
	if(ret == -61) {
		flags = 1
	} else if (ret < 0) {
		return CephError(ret)
	}

	size = uint64(len(value))

	fmt.Print("Size: ")
	fmt.Println(size)
	fmt.Print("Flags: ")
	fmt.Println(flags)

	c_flags := C.int(flags)

	ret = C.ceph_setxattr(mount.mount, c_path, c_name, unsafe.Pointer(c_value), *((*C.size_t)(&size)), c_flags)
	if(ret < 0) {
		return CephError(ret)
	}

	return nil
}