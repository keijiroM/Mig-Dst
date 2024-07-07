#include <iostream>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "./socket.h"



static int ConnectSocket(const int& port_number, const char* ip) {
	sockaddr_in saddr;
	int         fd;

	if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		std::cerr << "socket" << std::endl;
		exit(EXIT_FAILURE);
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family      = AF_INET;
	saddr.sin_port        = htons(port_number);
	saddr.sin_addr.s_addr = inet_addr(ip);

	while (true) {
		int ret = connect(fd, (sockaddr*)&saddr, sizeof(saddr));
		if (ret == 0) {
			break;
		}
        else {
			std::cerr << "connect()" << std::endl;
			// close(fd);
			// exit(EXIT_FAILURE);
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}

	return fd;
}



// void OpenSocket(const std::string& id, SocketFD& socket_fd) {
void OpenSocket(const std::string& id, const int& number_of_threads, SocketFD& socket_fd) {
	int port_number = 20000 + (atoi(id.c_str()) * 100);
	const char* ip = "192.168.202.204";

	socket_fd.main_fd = ConnectSocket(port_number++, ip);
#ifndef IDLE
	socket_fd.kv_fd = ConnectSocket(port_number++, ip);
#endif
	// for (int i = 0; i < 2; ++i) {
	for (int i = 0; i < 2*number_of_threads; ++i) {
		socket_fd.sst_fd.emplace_back(ConnectSocket(port_number++, ip));
	}
}



// void CloseSocket(const SocketFD& socket_fd) {
void CloseSocket(const int& number_of_threads, const SocketFD& socket_fd) {
	close(socket_fd.main_fd);
#ifndef IDLE
	close(socket_fd.kv_fd);
#endif
	// for (int i = 0; i < 2; ++i) {
	for (int i = 0; i < 2*number_of_threads; ++i) {
		close(socket_fd.sst_fd[i]);
	}
}



void SendFlag(const int& fd, const short& flag) {
	send(fd, &flag, sizeof(short), 0);
}



short RecvFlag(const int& fd, const std::string& flag_name) {
	short flag = 0;

	if (recv(fd, &flag, sizeof(short), 0) < 0) {
		std::perror(("recv(" + flag_name + ")").c_str());
		exit(EXIT_FAILURE);
	}

	return flag;
}
