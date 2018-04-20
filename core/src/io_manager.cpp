extern"C"{
#include <log.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
}


#include <thread>
#include <atomic>

#include <io_manager.hpp>
#include <metadata_manager.hpp>

void my_free (void *data, void *hint) {
	free (data);
}

int IOManager::init_sockets(int num_workers, int starting_port) {	
	fprintf(stderr, "master has %d workers\n", num_workers);	
	this->context = zmq_ctx_new();

	for (int i = 0; i < num_workers; i++) {
		std::string address = "tcp://127.0.0.1:" + std::to_string(starting_port+i);
		fprintf(stderr, "master bind a socket to address:%s\n", address.c_str());
		void *req = zmq_socket(context, ZMQ_REQ);
		int res = zmq_bind(req, address.c_str());
		if (res != 0) {
			fprintf(stderr, "master fails to bind a socket to address:%s\n", address.c_str());
		}
		this->sockets.push_back(req);
	}
	fprintf(stderr, "master initialize sockets successfully\n");
}


// send a char '1' to all the workers and wait for reply 
int IOManager::init() {
	fprintf(stderr, "master starts to send init msg\n");
	for(int i = 0; i < this->sockets.size(); i++) {
		char *buf = (char*)malloc(sizeof(char)*1);
		if (buf == NULL) {
			fprintf(stderr, "malloc buf fails\n");
			exit(-1);
		}
		fprintf(stderr, "malloc buf succeeds\n");
		buf[0] = '1';
		fprintf(stderr, "fill buf with '1'\n");
		zmq_msg_t msg;
		zmq_msg_init_data(&msg, buf, 1, my_free, NULL);
		int sent_size = zmq_msg_send(&msg, sockets[i], 0);
		if (sent_size == -1) {
			fprintf(stderr, "master send init fails with %s\n", strerror(errno));
		}
		assert(sent_size == 1);
		fprintf(stderr, "master sent init msg to socket:%d\n", i);
	}

	for(int i = 0; i < this->sockets.size(); i++) {
		zmq_msg_t msg;
		int rc = zmq_msg_init(&msg);
		if (rc != 0) {
			fprintf(stderr, "master receive init reply init fails with %s\n", strerror(errno));
		}
		int recv_size = zmq_msg_recv(&msg, sockets[i], 0);
		if (recv_size == -1) {
			fprintf(stderr, "master receive init reply fails with %s\n", strerror(errno));
		}

		char *data = (char*) zmq_msg_data(&msg);
		if (strcmp(data, "ok") == 0){
			fprintf(stderr, "master receive `ok`\n");
		}
		else {
			fprintf(stderr, "master did not receive `ok` but `%s`\n", data);
		}
	}

	fprintf(stderr, "all sockets has already connected to master!\n");
}

ssize_t IOManager::read_small_file(ino_t ino, char *buf, size_t size, off_t offset) {
	log_msg("io manager read small file from inode number:%lu, size:%lu, off_t:%lu",ino, size, offset);
	ssize_t ret = -1;
	MetadataManager &mm = MetadataManager::get_instance();
	size_t file_size = mm.get_size(ino);
	
	if (offset >= file_size) {
		return 0;
	}
	if (size == 0) {
		return 0;
	}

	if (offset + size >= file_size) {
		ret = file_size - offset;
	}

	ret = size;

	// first we start N threads to send msg type information to workers
	// to indicate that the master needs to read small file
	// then we send file inode number, size and offset to workers
	// to notify them what part I want to read
	std::vector<std::thread> threads;
	std::string ino_size_offset = std::to_string(ino) + " " + std::to_string(size) + " " + std::to_string(offset);
	size_t len = ino_size_offset.length();
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread send_thread(
		[=](){
			char *data = (char*) malloc(sizeof(char) * 1);
			data[0] = '2';
			zmq_msg_t msg;
			int rc = zmq_msg_init_data(&msg, data, 1, my_free, NULL);
			assert(rc == 0);
			int sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
			if (sent_size == -1) {
				log_msg("io manager send read small file msg type fails with:%s\n", strerror(errno));
			}
			log_msg("io mananger send read small file msg type with %d bytes\n", sent_size);
			char *data2 = (char*) malloc(sizeof(char) * len);
			memcpy(data2, ino_size_offset.c_str(), len);
			rc = zmq_msg_init_data(&msg, data2, len, my_free, NULL);
			assert(rc == 0);
			sent_size = zmq_msg_send(&msg, this->sockets[i], 0);
			if (sent_size == -1) {
				log_msg("io mananger socket %d read small file send fails with error:%s\n", i, strerror(errno)); 
			}
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(send_thread));
	}
	
	for(int i = 0; i < this->sockets.size(); i++) {
		threads[i].join();
	}

	threads.clear();	

	// to ensure that only thread copy the data into buffer
	std::atomic<bool> flag(true);
	// we start N threads to receive data from workers
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread recv_thread(
		[&, this, i, buf](){
			zmq_msg_t msg;
			int rc = zmq_msg_init(&msg);
			assert(rc == 0);
			int recv_size = zmq_msg_recv(&msg, this->sockets[i], 0);
			if (recv_size == -1) {
				log_msg("io manager socket %d receive small file fails with error:%s\n", i, strerror(errno));
				return;
			}
			char *data = (char*) zmq_msg_data(&msg);
			if (flag.exchange(false)) {
				memcpy(buf, data, recv_size);
			}	
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(recv_thread));
	}

	for(int i = 0; i < this->sockets.size(); i++) {
		threads[i].join();
	}
	
	return ret;
}

ssize_t IOManager::write_small_file(ino_t ino, const char *buf, size_t size, off_t offset) {
	log_msg("io manager write small file from inode number:%lu, size:%lu, off_t:%lu",ino, size, offset);
	
	// first we start N threads to send msg type information to workers
	// to indicate that the master needs to read small file
	// then we send file inode number, size and offset to workers
	// to notify them what part I want to write
	std::string ino_size_offset = std::to_string(ino) + " " + std::to_string(size) + " " + std::to_string(offset);
	size_t len = ino_size_offset.length();
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread send_thread(
		[=](){
			char *data = (char*) malloc(sizeof(char) * 1);
			data[0] = '2';
			zmq_msg_t msg;
			int rc = zmq_msg_init_data(&msg, data, 1, my_free, NULL);
			assert(rc == 0);
			int sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
			if (sent_size == -1) {
				log_msg("io manager send write small file msg type fails with:%s\n", strerror(errno));
			}
			log_msg("io mananger send write small file msg type with %d bytes\n", sent_size);
			char *data2 = (char*) malloc(sizeof(char) * len);
			memcpy(data2, ino_size_offset.c_str(), len);
			rc = zmq_msg_init_data(&msg, data2, len, my_free, NULL);
			assert(rc == 0);
			sent_size = zmq_msg_send(&msg, this->sockets[i], ZMQ_SNDMORE);
			if (sent_size == -1) {
				log_msg("io mananger socket %d write small file send metadata fails with error:%s\n", i, strerror(errno)); 
			}
			char *data3 = (char*) malloc(sizeof(char) * size);
			memcpy(data3, buf, size);
			rc = zmq_msg_init_data(&msg, data3, size, my_free, NULL);
			assert(rc == 0);
			sent_size = zmq_msg_send(&msg, this->sockets[i], 0);
			if (sent_size == -1) {
				log_msg("io manager socket %d write small file send file fails with error:%s\n", i, strerror(errno));
			}
			zmq_msg_close(&msg);
		});
	}
	
	std::vector<std::thread> recv_threads;
	// receive reply from workers
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread recv_thread(
		[=](){
			zmq_msg_t msg;
			int rc = zmq_msg_init(&msg);
			assert(rc == 0);
			int recv_size = zmq_msg_recv(&msg, this->sockets[i], 0);
			if (recv_size == -1) {
				log_msg("io manager socket %d receive small file fails with error:%s\n", i, strerror(errno));
			}
			zmq_msg_close(&msg);
		});
		recv_threads.push_back(std::move(recv_thread));
	}
	
	// wait for all workers to receive the data we want to write
	// since zmq socket is not thread safe, we have to ensure that these sockets all complete their operations before
	for(int i = 0; i < this->sockets.size(); i++) {
		recv_threads[i].join();
	}
	return size;	
}

ssize_t IOManager::read_large_file(ino_t ino, char *buf, size_t size, off_t offset) {
	log_msg("IOManager::read_large_file size:%lu, offset:%lu\n");
	int N = this->sockets.size();
	ssize_t ret = -1;
	// so we first have to find all the block we need to read
	// the block size of a file is 4096
	// so we need to find out how many blocks we need to read
	// Note that we don't necessarily return all the bytes in these blocks to users
	// since user may just want part of it
	
	MetadataManager &mm = MetadataManager::get_instance();
	// first we get the actual file size of ino
	size_t file_size = mm.get_size(ino);
	// if the offset is larger than the file size, we cannot read anything
	if (offset >= file_size) {
		return 0;
	}
	// if size if 0
	if (size == 0) {
		return 0;
	}

	if (offset + size >= file_size) {
		ret = file_size - offset;
	}

	ret = size;

	int first_block_idx = offset / BLOCK_SIZE;
	int last_block_idx = (offset + ret - 1) / BLOCK_SIZE;

	log_msg("IOManager::read_large_file reads from %dth block to %dth block\n", first_block_idx, last_block_idx);
	// so we need blocks from 'first_block_idx` to 'last_block_idx'
	// for every block, for every worker, we need to send a meesage to notify the worker should send back these things
	// note that we split every block into N-1 page. So for each page, there is a worker contains a parity page instead of a real page. We do not ask worker to send this page back
	std::vector<std::vector<int>> worker_msgs(N);
	for(int idx = first_block_idx; idx <= last_block_idx; idx++) {
		int worker_has_parity = idx % N;
		for(int i = 0; i < N; i++) {
			if (worker_has_parity != i) { 
				worker_msgs[i].push_back(idx);
			}
		}
	}

	// also note that BLOCK_SIZE % (N-1) may not be zero. Which means that for each page, we need to padding some 0 so that the extended block can be divided by (N-1). For convenice, we will always choose the worker with the largest index (if it holds a parity, then second largest) to hold the actual data rather than 0
	// so here we calculate how much bytes a page should have
	int new_block_size = ((BLOCK_SIZE / (N-1)) + 1) * (N-1);

	int new_page_size = BLOCK_SIZE / (N-1);

	int larger_page_size = new_page_size;
	int smaller_page_size = BLOCK_SIZE - larger_page_size * (N-2);

	std::vector<std::thread> threads;
	std::string filename_str = std::to_string(ino);
	std::string new_page_size_str = std::to_string(new_page_size);
	// then we send the msg to N workers
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread send_thread(	
		[=]{
			// the first message is message type
			char *msg_type = (char*) malloc(sizeof(char) * 1);
			memcpy(msg_type, "3", 1);
			zmq_msg_t msg;
			int rc = zmq_msg_init_data(&msg, msg_type, 1, my_free, NULL);
			if (rc != 0) {
				log_msg("IOManager::read_large_file zmq_msg_init_data 1 fails with:%s\n", strerror(errno));
			}
			int sent_size = zmq_msg_send(&msg, this->sockets[i], ZMQ_SNDMORE); 
			if (sent_size == -1) {
				log_msg("IOManager::read_large_file zmq_msg_send 1 fails with:%s\n", strerror(errno));
			}

			log_msg("IOManager::read_large_file zmq_msg_send 1 succeeds\n");
			// the first meesage is message type
			
			// the second message is filename (inode number)
			char *msg_filename = (char*) malloc(sizeof(char) * filename_str.length());
			memcpy(msg_filename, filename_str.c_str(), filename_str.length());	
			rc = zmq_msg_init_data(&msg, msg_filename, filename_str.length(), my_free, NULL);
			if (rc != 0) {
				log_msg("IOManager::read_large_file zmq_msg_init_data 2 fails with:%s\n", strerror(errno));
			}
			sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
			if (sent_size == -1) {
				log_msg("IOManager::read_large_file zmq_msg_send 2 fails with:%s\n", strerror(errno));
			}

			log_msg("IOManager::read_large_file zmq_msg_send 2 succeeds\n");
			// the second message is filename (inode number); 
			
			for(int j = 0; j < worker_msgs[i].size(); j++) {
				int block_idx = worker_msgs[i][j];
				std::string idx_size_str = std::to_string(block_idx) + " " + new_page_size_str;
				char *msg_idx_size = (char*) malloc(sizeof(char) * idx_size_str.length());
				memcpy(msg_idx_size, idx_size_str.c_str(), idx_size_str.length());
				rc = zmq_msg_init_data(&msg, msg_idx_size, idx_size_str.length(), my_free, NULL);
				if (rc != 0) {
					log_msg("IOManager::read_large_file zmq_msg_ini_data 3 fails with:%s\n", strerror(errno));
				}
				if (j != (worker_msgs[i].size()-1)) {
					sent_size = zmq_msg_send(&msg, this->sockets[i], ZMQ_SNDMORE);
				}else {
					sent_size = zmq_msg_send(&msg, this->sockets[i], 0);
				}
				if (sent_size == -1) {
					log_msg("IOManager::read_large_file zmq_msg_send 3 fails with:%s\n", strerror(errno));
				}
			}
			log_msg("IOManager::read_large_file zmq_msg_send 3 succeeds\n");
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(send_thread));
	}

	// now we wait for all threads terminate
	for(int i = 0; i < threads.size(); i++) {
		threads[i].join();
	}

	threads.clear();

	// now we collect the data sent from all workers
	// the data from each worker will still be in ascending order
	char *collect_buffer = (char*) malloc(sizeof(char) * (last_block_idx - first_block_idx + 1) * BLOCK_SIZE);
	for(int i = 0; i < this->sockets.size(); i++) {
		std::thread recv_thread(
		[=]{
			zmq_msg_t msg;
			int rc = zmq_msg_init(&msg);
			assert(rc == 0);
			int recv_size = zmq_msg_recv(&msg, this->sockets[i], 0);
			if (recv_size == -1) {
				log_msg("IOManager::read_large_file receive fails with error:%s\n", strerror(errno));
			}
			char *data = (char*) zmq_msg_data(&msg);
			int num_page_request = worker_msgs[i].size();
			int num_bytes_request = new_page_size * num_page_request;
			if (recv_size != num_bytes_request) {
				log_msg("IOManager::read_large_file receive bytes not the same as requested\n");
			}
			for(int j  = 0; j < worker_msgs[i].size(); j++) {
				int offset_in_block = new_page_size * j;
				int block_idx = worker_msgs[i][j];
				int effective_size = 0;
				off_t offset_in_collect_buffer = 0;	
				// base on the block indx, we can calculate where this thread should put the page recived at
				// be careful that not all bytes of the page should be place on the collect_buffer
				// since we pad 0 so that the block can be divided by N-1
				// so there are two kinds of pages, the first kind of page will have less effective bytes than the second kind of page. The largest worker (if it doesn not hold parity block) will hold the sceond kind of page
				// so here we first check whether this page should be a small page or a large page
				// the only possible workers who may hold the larger one is the last two workers with index N-2 and N-1.
				// if the current socket is receiving data from worker N-1.
				// then it holds a smaller page
				if (i == (N - 1)) {
					effective_size = smaller_page_size;
					offset_in_collect_buffer = block_idx * BLOCK_SIZE + (N - 2) * larger_page_size;
				}
				// if the current socket is receiving data from worker N-2 and worker N-1 holds the parity block
				else if ((i == (N - 2)) && (block_idx % N) == (N - 1)) {
					effective_size = smaller_page_size;
					offset_in_collect_buffer = block_idx * BLOCK_SIZE + (N - 2) * larger_page_size; 
				}
				else {
					effective_size = larger_page_size;
					offset_in_collect_buffer = block_idx * BLOCK_SIZE + i * larger_page_size;
				}

				memcpy(collect_buffer + offset_in_collect_buffer, data + offset_in_block, effective_size);
			}
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(recv_thread));
	}


	// wait for all the data is copied to the collect_buffer
	for(int i = 0; i < N; i++) {
		threads[i].join();
	}
	
	// then we only copy the part that the user want from `collect_buffer` to `buf`
	int start = offset % BLOCK_SIZE;
	memcpy(buf, collect_buffer + start, ret);
	
	// free the buffer
	free(collect_buffer);
	return ret;
}


ssize_t IOManager::write_large_file(ino_t ino, const char *buf, size_t size, off_t offset) {
	log_msg("io manager write large file from inode number:%lu, size:%lu, off_t:%lu",ino, size, offset);
	int N = this->sockets.size();

	ssize_t ret;
	if (size == 0) {
		return 0;
	}

	ret = size;

	int first_block_idx = offset / BLOCK_SIZE;
	int last_block_idx = (offset + ret - 1) / BLOCK_SIZE;
	int num_blocks = (last_block_idx - first_block_idx + 1);

	size_t total_bytes = (last_block_idx - first_block_idx + 1) * BLOCK_SIZE;
	off_t round_down_offset = first_block_idx * BLOCK_SIZE;

	log_msg("IOManager::read_large_file reads from %dth block to %dth block\n", first_block_idx, last_block_idx);

	char* collect_buffer = NULL;

	// there are two possible situations
	// 1 general situation
	// 2 offset is at the beginning of a block and size can be divided BLOCK_SIZE
	// In the second situtaion, we don't need to first fetch blocks from workers, modify them in place, and then partition them into N-1 pages, finally generate the parity block and distribute them
	// we can save the fetch at the first
	
	int new_block_size = ((BLOCK_SIZE / (N-1)) + 1) * (N-1);

	int new_page_size = BLOCK_SIZE / (N-1);

	int larger_page_size = new_page_size;
	int smaller_page_size = BLOCK_SIZE - larger_page_size * (N-2);
	
		
	// so here we first check whether this is the second situation
	// if it is then do nothing, becuase we don't need to fetch data from remote workers
	if ((offset % BLOCK_SIZE == 0) && (size % BLOCK_SIZE == 0)) {
		collect_buffer = (char*)buf;
	}
	// generall situaion
	// then we need to send msg to remotes workers, here we can reuse read_large_file function to achieve it
	else {
		collect_buffer = (char*) malloc(sizeof(char) * total_bytes);
		int recv = read_large_file(ino, collect_buffer, total_bytes, round_down_offset); 
		if (recv != total_bytes) {
			log_msg("IOManager::write_large_file collect bytes from workers expected:%lu, real:%lu bytes\n");
		}
		int offset_in_buffer = offset % BLOCK_SIZE;
		// apply the write from the user to `collect_buffer`
		memcpy(collect_buffer + offset_in_buffer, buf, size);
	}

	// we need to decide, for each block, what each of worker will get
	// so for each block and each worker, the worker will get a pair of (offset, size) in collect buffer
	// we also need to generate parity block.
	//
	// the first vector is for each block
	// the nested vector if for each page
	std::vector<std::vector<std::pair<off_t, size_t>>> block_partition(num_blocks);
	std::vector<std::string> parity_pages;
	for(int idx = first_block_idx; idx <= last_block_idx; idx++) {
		// idx in `block_partition`
		int idx_in_vec = idx - first_block_idx;
		int worker_has_parity = idx % N;
		// if the `N-1`th worker holds the parity, then `N-2`th worker holds larger_page
		int worker_has_smaller_page = (worker_has_parity == (N-1)) ? N-2 : N-1;
		for(int i = 0; i < N; i++) {
			off_t offset_in_buffer = 0;
			size_t size_in_buffer = 0;
			// do nothing for worker_has_parity
			if (i == worker_has_parity) {
				// in this situation offset doesn't not mean offset in `collect buffer`
				// but means the index in `parity_pages`
				offset_in_buffer = idx_in_vec;
			}
			// when `i`th worker holds a larger page
			// i must be N-1 or N-2
			// but no matter what `i` is, it must skip `N-1` smaller pages
			else if (i == worker_has_smaller_page) {
				offset_in_buffer = (N-1) * larger_page_size;
				size_in_buffer = smaller_page_size;
			}
			// when `i` the worker holds a smaller page
			// it must be < N-2
			else {
				offset_in_buffer = i * larger_page_size;
				size_in_buffer = larger_page_size;
			}
			block_partition[idx_in_vec].push_back(std::make_pair(offset_in_buffer, size_in_buffer));
		}
	}


	// here we generate the parity page
	for(int idx = first_block_idx; idx <= last_block_idx; idx++) {
		int idx_in_vec = idx - first_block_idx;
		int parity_page_size = new_page_size;
		
		// first create a new page
		std::string parity(new_page_size, '\0');
		// for all the pages in current block
		const std::vector<std::pair<off_t, size_t>> &pages = block_partition[idx_in_vec];
		for(int i = 0; i < pages.size(); i++) {
			off_t offset_in_buffer = pages[i].first;
			size_t size_in_buffer = pages[i].second;

			int byte_idx = 0;
			while(byte_idx < size_in_buffer) {
				parity[byte_idx] ^= collect_buffer[idx_in_vec * BLOCK_SIZE + byte_idx];
				byte_idx++;
			}
		}
		parity_pages.push_back(std::move(parity));
	}

	
	// then we start to send data to remote workers
	std::vector<std::thread> threads;
	std::string filename_str = std::to_string(ino);
	for(int i = 0; i < N; i++) {
		std::thread send_thread(
		[=]{
			// the first message is message type
			char *msg_type = (char*) malloc(sizeof(char) * 1);
			memcpy(msg_type, "5", 1);
			zmq_msg_t msg;
			int rc = zmq_msg_init_data(&msg, msg_type, 1, my_free, NULL);
			if (rc != 0) {
				log_msg("IOManager::write_large_file zmq_msg_init_data 1 fails with:%s\n", strerror(errno));
			}
			int sent_size = zmq_msg_send(&msg, this->sockets[i], ZMQ_SNDMORE); 
			if (sent_size == -1) {
				log_msg("IOManager::write_large_file zmq_msg_send 1 fails with:%s\n", strerror(errno));
			}
			// the first message is message type

			// the second message is filename (inode number)
			char *msg_filename = (char*) malloc(sizeof(char) * filename_str.length());
			memcpy(msg_filename, filename_str.c_str(), filename_str.length());	
			rc = zmq_msg_init_data(&msg, msg_filename, filename_str.length(), my_free, NULL);
			if (rc != 0) {
				log_msg("IOManager::write_large_file zmq_msg_init_data 2 fails with:%s\n", strerror(errno));
			}
			sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
			if (sent_size == -1) {
				log_msg("IOManager::write_large_file zmq_msg_send 2 fails with:%s\n", strerror(errno));
			}

			log_msg("IOManager::write_large_file zmq_msg_send 2 succeeds\n");
			// the second message is filename (inode number); 
			
			// then we iterate all the page we need to send
			// we first send description about the data
			// we then send the real data
			for(int idx = 0; idx < block_partition.size(); idx++) {
				off_t offset_in_buffer = block_partition[idx][i].first;
				size_t size_in_buffer = block_partition[idx][i].second;

				int block_index = first_block_idx + idx;
				int worker_has_parity = block_index % N;
				std::string idx_size_str = std::to_string(block_index) + " " + std::to_string(new_page_size);
			
				// the third meesage is description about the data
				char *msg_filename = (char*) malloc(sizeof(char) * filename_str.length());
				memcpy(msg_filename, filename_str.c_str(), filename_str.length());	
				rc = zmq_msg_init_data(&msg, msg_filename, filename_str.length(), my_free, NULL);
				if (rc != 0) {
					log_msg("IOManager::write_large_file zmq_msg_init_data 3 fails with:%s\n", strerror(errno));
				}
				sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
				if (sent_size == -1) {
					log_msg("IOManager::write_large_file zmq_msg_send 3 fails with:%s\n", strerror(errno));
				}

				log_msg("IOManager::write_large_file zmq_msg_send 3 succeeds\n");
				// the third message is description about the data
				//
				// the fourth message is the real data
				if (i == worker_has_parity) {
					char *data = (char*) malloc(sizeof(char) * parity_pages[idx].length());
					memcpy(data, parity_pages[idx].c_str(), parity_pages[idx].length());
					rc = zmq_msg_init_data(&msg, data, parity_pages[idx].length(), my_free, NULL);
					if (rc != 0) {
						log_msg("IOManager::write_large_file zmq_msg_init_data 4 fails with:%s\n", strerror(errno));
					}
				}
				else {
					char *data = (char*) malloc(sizeof(char) * size_in_buffer);
					memcpy(data, collect_buffer + offset_in_buffer, size_in_buffer);
					rc = zmq_msg_init_data(&msg, data, size_in_buffer, my_free, NULL);
					if (rc != 0) {
						log_msg("IOManager::write_large_file zmq_msg_init_data 5 fails with:%s\n", strerror(errno));
					}
				}
				sent_size = zmq_msg_send(&msg, sockets[i], ZMQ_SNDMORE);
				if (sent_size == -1) {
					log_msg("IOManager::write_large_file zmq_msg_send 4 fails with:%s\n", strerror(errno));
				}
				// the fourth message is th real data
			}
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(send_thread));
	}

	for(int i = 0; i < N; i++) {
		threads[i].join();
	}

	threads.clear();

	for(int i = 0; i < N; i++) {
		std::thread recv_thread(
		[=]{
			zmq_msg_t msg;
			int rc = zmq_msg_init(&msg);
			assert(rc == 0);
			int recv_size = zmq_msg_recv(&msg, this->sockets[i], 0);
			if (recv_size == -1) {
				log_msg("io manager socket %d receive small file fails with error:%s\n", i, strerror(errno));
			}
			zmq_msg_close(&msg);
		});
		threads.push_back(std::move(recv_thread));
	}

	for(int i = 0; i < N; i++) {
		threads[i].join();
	}
	return 0;
}
