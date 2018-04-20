#pragma once
#include <zmq.h>

#include <vector>
#include <string>


#define BLOCK_SIZE 4096

class IOManager {
	public:
		static IOManager &get_instance() {
			static IOManager io_manager;
			return io_manager;
		}

		int init_sockets(int num_workers, int starting_port);

		int init();

		int get_attr();

		ssize_t read_small_file(ino_t, char*, size_t, off_t);

		ssize_t read_large_file(ino_t, char*, size_t, off_t);

		ssize_t write_small_file(ino_t, const char*, size_t, off_t);

		ssize_t write_large_file(ino_t, const char*, size_t, off_t);

	private:
		IOManager() {}

		~IOManager() {}

		void *context;
		std::vector<void*> sockets;
};
