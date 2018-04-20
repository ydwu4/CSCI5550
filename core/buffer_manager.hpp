#pragma once
extern"C"{
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
}
#include <unordered_map>
#include <memory>


enum FileType { Undecided, Small, Large};

enum FileLocation { Local, Remote};

class FileBuffer{
public:
	FileBuffer(size_t theta);

	FileBuffer(const FileBuffer &) = delete;

	FileBuffer(FileBuffer &&) = default;

	size_t get_all_bytes();

	size_t get_used_bytes();

	size_t write_bytes(const char* buf, size_t size, off_t offset);

	void set_type(FileType type);

	FileType get_type();

	void set_location(FileLocation loc);

	FileLocation get_location();

	void clear();
	
	char* get_data();

	char* reset();

private:
	FileType file_type;
	FileLocation file_loc;
	std::unique_ptr<char> bytes;
	size_t used_bytes;
	const size_t all_bytes;
};


class BufferManager {
	public:
		static BufferManager &get_instance() {
			static BufferManager buffer_manager;
			return buffer_manager;
		}

		ssize_t read(ino_t ino, char *buf, size_t size, off_t offset); 

		ssize_t write(ino_t ino, const char *buf, size_t size, off_t offset);	

		int close(ino_t ino);

	private:
		BufferManager();

		~BufferManager();

		std::unordered_map<ino_t, std::unique_ptr<FileBuffer>> ino_to_buffer;

		size_t theta;
};
