extern"C"{
#include <log.h>
}
#include <metadata_manager.hpp>

size_t MetadataManager::get_size(ino_t ino) {
	size_t res = ino_to_size[ino];
	log_msg("MetadataManager get_size for inode number:%lu, result:%lu\n", ino, res);
	return res;
}

size_t MetadataManager::increase_size(ino_t ino, size_t size) {
	log_msg("MetadataManager increase_size for inode number:%lu, previous:%lu, now:%lu\n", ino_to_size[ino], ino_to_size[ino] + size);
	ino_to_size[ino] += size;
	return ino_to_size[ino];
}

size_t MetadataManager::decrease_size(ino_t ino, size_t size) {
	log_msg("MetadataManager increase_size for inode number:%lu, previous:%lu, now:%lu\n", ino_to_size[ino], ino_to_size[ino] - size);
	ino_to_size[ino] -= size;
	return ino_to_size[ino];
}

size_t MetadataManager::set_size(ino_t ino, size_t size) {
	log_msg("MetadataManager set_size for inode number:%lu, previous:%lu, now:%lu\n", ino_to_size[ino], size);
	ino_to_size[ino] = size;
	return ino_to_size[ino];
}

bool MetadataManager::remove_size(ino_t ino) {
	int res = ino_to_size.erase(ino);
	return res==1;
}
