extern"C"{
#include <log.h>
#include <params.h>
}

#include <assert.h>
#include <fuse.h>
#include <string.h>

#include <io_manager.hpp>
#include <buffer_manager.hpp>

// at first we cannot be sure whether the file is small file or large file
// and we buffer it in local
FileBuffer::FileBuffer(size_t theta) : all_bytes(theta) {
	this->bytes.reset(new char[theta]);
	memset(this->bytes.get(), '\0', theta);
	this->file_loc = FileLocation::Local;
	this->file_type = FileType::Undecided;
	this->used_bytes = 0;
}


// the caller should check whether the file buffer can contain the data
// so when we enter this function
// size + offset <= theta
size_t FileBuffer::write_bytes(const char* buf, size_t size, off_t offset) {
	log_msg("FileBuffer::write_bytes size:%lu, offset:%lu\n", size, offset);
	assert(size + offset <= this->all_bytes);
	char* data = get_data();
	memcpy(data + offset, buf, size);
	this->used_bytes = this->used_bytes > (offset + size) ? this->used_bytes : (offset + size);
	return size;
}

size_t FileBuffer::get_used_bytes() {
	return this->used_bytes;
}

size_t FileBuffer::get_all_bytes() {
	return this->all_bytes;
}

void FileBuffer::set_type(FileType type) {
	this->file_type = type;
}

FileType FileBuffer::get_type() {
	return this->file_type;
}

void FileBuffer::set_location(FileLocation loc) {
	this->file_loc = loc;
}

FileLocation FileBuffer::get_location() {
	return this->file_loc;
}

void FileBuffer::clear() {
	this->bytes.reset(nullptr);	
	this->used_bytes = 0;
}

char* FileBuffer::get_data() {
	this->bytes.get();
}

char* FileBuffer::reset() {
	this->bytes.reset(new char[this->all_bytes]);
	this->used_bytes = 0;
}

BufferManager::BufferManager(){
	this->theta = BB_DATA->theta;
	fprintf(stderr, "BufferManager theta:%lu\n", this->theta);
	fprintf(stderr, "BufferManager completes initialization\n");
}

BufferManager::~BufferManager(){
	log_msg("BufferManager destructor\n");
}

// when we want to read a file, we takes the folloing steps
// 0. It is possible that the file hasn't been write before
//    then we first create an entry in `ino_to_buffer`
// 1. check whether it is at local or remote
// 2. if the file is at local
//    fill in buf with `size` data, done
// 3. if the file is at remote
// 	  check whether it is a small file or large file
// 4. if the file is a small file
// 	  issue small_file_request via IO Manager with inode number, size, offset
// 5. if the file is a large file
// 	  issue large_file_request via IO Manager with inode number, size, offset
ssize_t BufferManager::read(ino_t ino, char* buf, size_t size, off_t offset) {
	log_msg("BufferManager read ino:%lu, size:%lu, off_t:%lu\n", ino, size, offset);
	ssize_t ret = -1;

	// first check whether this file already exists
	if (this->ino_to_buffer.find(ino) == this->ino_to_buffer.end()) {
	//	std::unique_ptr<FileBuffer> fb = std::make_unique<FileBuffer>(this->theta);
	//	ino_to_buffer.insert(std::make_pair(ino, std::move(fb)));
		return 0;
	}
	// so we ensure that we can get a valid file buffer
	size_t used_bytes = this->ino_to_buffer[ino]->get_used_bytes();
	char* data = this->ino_to_buffer[ino]->get_data();
	
	// if the file is at local
	// the file must be undecided
	if (this->ino_to_buffer[ino]->get_location() == FileLocation::Local) {
		if (this->ino_to_buffer[ino]->get_type() != FileType::Undecided) {
			log_msg("[ERROR] The file is at local, but the type is not undecided!\n");
		}
		// if offset already passed what file buffer has
		if (offset >= used_bytes) {
			return 0;
		}
		// if offset + size passed what file buffer has
		if (offset + size >= used_bytes) {
			ret = used_bytes - offset;
			memcpy(buf, data + offset, ret);
			return ret;
		}
		// if offset + size < used_bytes
		ret = size;
		memcpy(buf, data + offset, ret);
		return ret;
	}
	// if the file is at remote
	// then the file is either small or large, cannot be undecided
	else {
		IOManager &io_manager = IOManager::get_instance();
		if (this->ino_to_buffer[ino]->get_type() == FileType::Small) {
			ret = io_manager.read_small_file(ino, buf, size, offset);
		}
		else if (this->ino_to_buffer[ino]->get_type() == FileType::Large) {
			ret = io_manager.read_large_file(ino, buf, size, offset);	
		}
		else {
			log_msg("[ERROR] The file is at remote, but the type is undecided!\n");
		}
		return ret;
	}
}


// when we want to write a file, we takes the folloing steps
// 0. It is possible that the file hasn't been write before
//    then we first create an entry in `ino_to_buffer`
// 1. check whether it is at local or remote
// 2. if the file is at remote
// 	  check whether it is a small file or large file
// 3. if the file is a small file
//    issue small_file_request via IO Manager with inode number, size, offset
// 4. if the file is a large file
// 	  issue large_file_request via IO Manager with inode number, size, offset
// 5. if the file is at local
//    fill in buf with `size` data,
ssize_t BufferManager::write(ino_t ino, const char* buf, size_t size, off_t offset) {
	log_msg("BufferManager write ino:%lu, size:%lu, off_t:%lu\n", ino, size, offset);

	ssize_t ret = -1;

	// first check whether this file already exists
	// if not, we put a buffer into map
	// if yes, keep going
	if (this->ino_to_buffer.find(ino) == this->ino_to_buffer.end()) {
		std::unique_ptr<FileBuffer> fb = std::make_unique<FileBuffer>(this->theta);
		ino_to_buffer.insert(std::make_pair(ino, std::move(fb)));
	}

	IOManager &io_manager = IOManager::get_instance();
	size_t all_bytes = this->ino_to_buffer[ino]->get_all_bytes();
	// if the file is at already remote
	// then we just send the new data immediately
	if (this->ino_to_buffer[ino]->get_location() == FileLocation::Remote) {
		// if the file is a large file
		if (this->ino_to_buffer[ino]->get_type() == FileType::Large) {
			ret = io_manager.write_large_file(ino, buf, size, offset);	
		}
		// if the file is a small file
		else if (this->ino_to_buffer[ino]->get_type() == FileType::Small) {
			ret = io_manager.write_small_file(ino, buf, size, offset);
		}
		else {
			log_msg("[ERROR] The file is at remote, but the type is undecided!\n");
		}
	}
	// if the file is at local
	else {
		// we first need to check whether the local buffer still can store it
		// if it can we just write to local buffer in memory
		if (offset + size <= this->ino_to_buffer[ino]->get_all_bytes()) {
			ret = this->ino_to_buffer[ino]->write_bytes(buf, size, offset);
		}
		// if it cannot be stored in local buffer any more
		else {
			// we allocate the sending buffer for the data both already in the buffer and current write request
			char* send_buffer = (char*) malloc(sizeof(char) * (offset + size));
			memset(send_buffer, '\0', offset + size);
			// first we copy the data in buffer to sending buffer 
			char* in_buf = this->ino_to_buffer[ino]->get_data();
			memcpy(send_buffer, in_buf, this->ino_to_buffer[ino]->get_used_bytes());
			// second we copy the data from the current write buffer to sending buffer
			memcpy(send_buffer + offset, buf, size);
			// then we need to clear the buffer
			this->ino_to_buffer[ino]->clear();
			// then we send the data
			ret = io_manager.write_large_file(ino, send_buffer, offset + size, 0);
			// finally we set the type of this file buffer to "Large file"
			// and set the location of this file buffer to "Remote"
			this->ino_to_buffer[ino]->set_type(FileType::Large);
			this->ino_to_buffer[ino]->set_location(FileLocation::Remote);
		}
	}
	return ret;
}

// this function will first check whether
// the file is at remote or at local
// 1. if the file is at remote
//    there cannot be anything in the local buffer, we just return
// 2. if the file is at local
//    there must be something in the local buffer, we tag it as small file and send things in the buffer to workers
int BufferManager::close(ino_t ino) {
	int ret = 0;
	// if the file is at remote
	// nothing we need to do
	if (this->ino_to_buffer[ino]->get_location() == FileLocation::Remote) {
		if (this->ino_to_buffer[ino]->get_used_bytes() != 0) {
			log_msg("[ERROR] The file is at remote but there is still data left in its buffer!\n");
		}
		return 0;
	}
	// if the file is at local
	// then its type must still be undecided
	// so it should be converted to 'Small' file, flush all the data in buffer to remote, and set location to "Remote"
	// finally, we set buffer's used bytes to 0 and clear the buffer
	else {
		size_t used_bytes = this->ino_to_buffer[ino]->get_used_bytes();
		char *data = this->ino_to_buffer[ino]->get_data();
		IOManager &im = IOManager::get_instance();
		ret = im.write_small_file(ino, data, used_bytes, 0);
		this->ino_to_buffer[ino]->set_type(FileType::Small);
		this->ino_to_buffer[ino]->set_location(FileLocation::Remote);
		// after we sent all the data
		// we clear the buffer
		this->ino_to_buffer[ino]->clear();
	}
	return ret;
}

