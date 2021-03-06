package storage

import (
	"fmt"
	"path/filepath"

	"go.uber.org/zap"
)

type S3BackedStore struct {
	url        string
	log        *zap.Logger
	backupName string
}

func NewS3BackendStore(url string, log *zap.Logger) *S3BackedStore {
	return &S3BackedStore{url: url, log: log}
}

func (s *S3BackedStore) SetBackupName(name string) {
	s.backupName = name
	if s.url[len(s.url)-1] != '/' {
		s.url += "/"
	}
	s.url += name
}

func (s *S3BackedStore) BackupPreCommand() []string {
	return nil
}

func (s *S3BackedStore) BackupStorageCommand(src string, host string, spaceID string) string {
	storageDir := s.url + "/" + "storage/" + host + "/" + spaceID + "/"
	return "aws s3 sync " + src + " " + storageDir
}

func (s S3BackedStore) BackupMetaCommand(src []string) string {
	metaDir := s.url + "/" + "meta/"
	return "aws s3 sync " + filepath.Dir(src[0]) + " " + metaDir
}

func (s S3BackedStore) BackupMetaFileCommand(src string) []string {
	return []string{"aws", "s3", "cp", src, s.url + "/"}
}

func (s S3BackedStore) RestoreMetaFileCommand(file string, dst string) []string {
	return []string{"aws", "s3", "cp", s.url + "/" + file, dst}
}

func (s S3BackedStore) RestoreMetaCommand(src []string, dst string) (string, []string) {
	metaDir := s.url + "/" + "meta/"
	var sstFiles []string
	for _, f := range src {
		file := dst + "/" + f
		sstFiles = append(sstFiles, file)
	}
	return fmt.Sprintf("aws s3 sync %s "+dst, metaDir), sstFiles
}
func (s S3BackedStore) RestoreStorageCommand(host string, spaceID []string, dst string) string {
	storageDir := s.url + "/storage/" + host + "/"

	return fmt.Sprintf("aws s3 sync %s "+dst, storageDir)
}
func (s S3BackedStore) RestoreMetaPreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s S3BackedStore) RestoreStoragePreCommand(dst string) string {
	return "rm -rf " + dst + " && mkdir -p " + dst
}
func (s S3BackedStore) URI() string {
	return s.url
}

func (s S3BackedStore) CheckCommand() string {
	return "aws s3 ls " + s.url
}
