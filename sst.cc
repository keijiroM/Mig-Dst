// Destination
#include <iostream>
#include <thread>
#include <sys/socket.h>
#include "./sst.h"
#include "./socket.h"
#include "./debug.h"
// #include <unistd.h> // for debug
// #include <sys/stat.h> // for debug

RunTime import_time, ingest_time;
moodycamel::ConcurrentQueue<std::string> file_queue;


void SstFile::RecvKeys(const int& fd, 
					   const uint64_t& number_of_entries, 
					   moodycamel::ConcurrentQueue<std::string>& key_queue) {
	for (uint64_t i = 0; i < number_of_entries; ++i) {
		std::string data;
		char buf[KEYLEN];

		size_t total_size = 0;
        while (total_size < KEYLEN) {
            ssize_t recv_size = recv(fd, buf, KEYLEN-total_size, 0);
            if (recv_size <= 0) {
                std::cerr << "recv()" << std::endl;
                exit(EXIT_FAILURE);
            }

            data.append(buf, recv_size);
            total_size += recv_size;
        }

		key_queue.enqueue(data);
	}
}



void SstFile::RecvValues(const int& fd, 
						 const uint64_t& number_of_entries, 
						 moodycamel::ConcurrentQueue<std::string>& value_queue) {
	for (uint64_t i = 0; i < number_of_entries; ++i) {
		std::string data;
		char buf[VALUELEN-1];

		size_t total_size = 0;
		while (total_size < VALUELEN-1) {
			ssize_t recv_size = recv(fd, buf, VALUELEN-1-total_size, 0);
			if (recv_size <= 0) {
				std::cerr << "recv()" << std::endl;
				exit(EXIT_FAILURE);
			}

			data.append(buf, recv_size);
			total_size += recv_size;
		}

		value_queue.enqueue(data);
	}
}



void SstFile::ImportKVPairs(const Options& options, 
							const uint64_t& number_of_entries, 
							const std::string& file_path, 
							moodycamel::ConcurrentQueue<std::string>& key_queue, 
							moodycamel::ConcurrentQueue<std::string>& value_queue) {
	SstFileWriter sst_file_writer(EnvOptions(), options);
	Status status = sst_file_writer.Open(file_path);
	AssertStatus(status, "ImportKVPairs(Open)");
	
	for (uint64_t i = 0; i < number_of_entries; ++i) {
		std::string key;
		while (!key_queue.try_dequeue(key)) {
			std::this_thread::yield();
		}
		std::string value;
		while (!value_queue.try_dequeue(value)) {
        	std::this_thread::yield();
    	}


		import_time.start = std::chrono::system_clock::now(); // for debug


		status = sst_file_writer.Put(key, value);
		AssertStatus(status, "ImportKVPairs(Put)");


		//for debug
		import_time.end = std::chrono::system_clock::now();
		import_time.time += ReturnRunTime(import_time.start, import_time.end);
	}

	status = sst_file_writer.Finish();
	AssertStatus(status, "ImportKVPairs(Finish)");

	file_queue.enqueue(file_path);

	
	// struct stat stat_buf;
	// if (stat(file_path.c_str(), &stat_buf) == 0) {
	//    std::cout << "File size: " << stat_buf.st_size << " bytes" << std::endl;
	// } else {
	//	std::cerr << "Error: Could not retrieve file stats" << std::endl;
    // }
}



/*
void SstFile::IngestSstFiles(DB* db, const int& file_level) {
	std::string file_path;
	while (!file_queue.try_dequeue(file_path)) {
		std::this_thread::yield();
	}


	ingest_time.start = std::chrono::system_clock::now();


	std::vector<std::string> ingest_file_paths;
	ingest_file_paths.push_back(file_path);

	IngestExternalFileOptions ifo;
	ifo.move_files = true;
	ifo.external_level = file_level;

	Status status = db->IngestExternalFile(ingest_file_paths, ifo);
	AssertStatus(status, "IngestSstFiles(IngestExternalFile)");


	// for debug
	ingest_time.end = std::chrono::system_clock::now();
	ingest_time.time += ReturnRunTime(ingest_time.start, ingest_time.end);
}
*/



static void IngestSstFiles(DB* db, 
						   const std::vector<SstFileData>& file_datas, 
						   const int& number_of_files, 
						   const int& number_of_threads, 
						   const int& thread_number) {
	// int file_number = 0;
	int file_number = thread_number;

	while (file_number < number_of_files) {
		std::string file_path;
		while (!file_queue.try_dequeue(file_path)) {
			std::this_thread::yield();
		}


		ingest_time.start = std::chrono::system_clock::now();


		std::vector<std::string> ingest_file_paths;
		ingest_file_paths.push_back(file_path);

		IngestExternalFileOptions ifo;
		ifo.move_files = true;
		ifo.external_level = file_datas[file_number].level;

		Status status = db->IngestExternalFile(ingest_file_paths, ifo);
		AssertStatus(status, "IngestSstFiles(IngestExternalFile)");


		// for debug
		ingest_time.end = std::chrono::system_clock::now();
		ingest_time.time += ReturnRunTime(ingest_time.start, ingest_time.end);
		

		std::cout << "file_number: " << file_number << std::endl;
		// ++file_number;
		file_number += number_of_threads;
	}
}



static void RecvSstFiles(TransferSstFilesArgs args, const int& thread_number) {
	// int file_number = 0;
	int file_number = thread_number;
	
	while (file_number < args.number_of_files) {
		SstFile sst_file;
		SstFileData file_data = args.file_datas[file_number];
		const std::string file_path = ReturnFilePath(args.dbname, 
													 file_data.level, 
													 file_data.on_disk, 
													 args.target_level, 
													 file_number);
		moodycamel::ConcurrentQueue<std::string> key_queue;
		moodycamel::ConcurrentQueue<std::string> value_queue;
		// moodycamel::ConcurrentQueue<std::string> file_queue;

		std::thread RecvKeysThread([&]{ sst_file.RecvKeys(args.sst_fd[2*thread_number], 
														  file_data.number_of_entries, 
														  std::ref(key_queue)); });
		std::thread RecvValuesThread([&]{ sst_file.RecvValues(args.sst_fd[1+2*thread_number], 
															  file_data.number_of_entries, 
															  std::ref(value_queue)); });
		std::thread ImportKVPairsThread([&]{ sst_file.ImportKVPairs(args.options, 
																	file_data.number_of_entries, 
																	file_path, 
																	std::ref(key_queue), 
																	std::ref(value_queue)); });
		// std::thread IngestSstFilesThread([&]{ sst_file.IngestSstFiles(args.db, file_data.level); });

    	RecvKeysThread.join();
    	RecvValuesThread.join();
    	ImportKVPairsThread.join();
    	// IngestSstFilesThread.join();


		// ++file_number;
		file_number += args.number_of_threads;
	}
}



// void TransferSstFiles(TransferSstFilesArgs args, bool& share) {
void TransferSstFiles(TransferSstFilesArgs args) {
    std::cout << "TransferSstFilesThread is started." << std::endl;


    // std::thread RecvSstFilesThread([]{ RecvSstFiles(args); });
	// std::thread IngestSstFilesThread([]{ IngestSstFiles(args.db, args.file_datas, args.number_of_files); });
	// std::thread RecvSstFilesThread(RecvSstFiles, args);
	// std::thread IngestSstFilesThread(IngestSstFiles, args.db, args.file_datas, args.number_of_files);
	// RecvSstFilesThread.join();
	// IngestSstFilesThread.join();


	std::vector<std::thread> recv_threads;
	std::vector<std::thread> ingest_threads;
	for (int i = 0; i < args.number_of_threads; ++i) {
		recv_threads.emplace_back(RecvSstFiles, args, i);
		ingest_threads.emplace_back(IngestSstFiles, 
									args.db, 
									args.file_datas, 
									args.number_of_files, 
									args.number_of_threads, 
									i);
	}
	for (auto& t : recv_threads) {
		t.join();
	}
	for (auto& t : ingest_threads) {
        t.join();
    }


	// share = true;


	std::cout << "ImportKVPairs : " << import_time.time / 1000000 << " sec" << std::endl;
	std::cout << "IngestSstFiles: " << ingest_time.time / 1000000 << " sec" << std::endl;


	std::cout << "TransferSstFilesThread is ended." << std::endl;
}
