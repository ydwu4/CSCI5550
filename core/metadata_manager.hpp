#pragma once
extern"C"{
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
}

#include <unordered_map>

class MetadataManager {
	public:
		static MetadataManager &get_instance() {
			static MetadataManager metadata_manager;
			return metadata_manager;
		}

		size_t get_size(ino_t);

		size_t increase_size(ino_t, size_t);

		size_t decrease_size(ino_t, size_t);

		size_t set_size(ino_t, size_t);

		bool remove_size(ino_t);

	private:

		MetadataManager(){}

		std::unordered_map<ino_t, size_t> ino_to_size;
};
