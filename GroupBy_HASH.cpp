#include<iostream>
#include<fstream>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include<mpi.h>
#include<omp.h>
#include <cstdio>
using namespace std;

#define BLOCK 1024*1024
#define FLAGS O_WRONLY | O_CREAT | O_TRUNC
#define MODE S_IRWXU | S_IXGRP | S_IROTH | S_IXOTH
int PNUM = 0;
int LOOP = 0;
char* NAME = NULL;

int comm_sz;
int my_rank;
int totalSize;
int blockSize;
int *mark;
int *mark_collection;

void readData(string path, int64_t *block)
{
	ifstream file(path.data(), ios_base::binary);
	file.seekg(my_rank*blockSize * sizeof(int64_t));
	file.read((char*)block, blockSize * sizeof(int64_t));
	file.close();
}


void check(int64_t* data, int a, int b, int pnum)
{
	int k = a;
	if (b<a)
		return;
	while (data[k] <= data[k + 1] && k != b)
		k++;
	if (k == b)
		cout << pnum << " is true" << endl;
	else
		cout << pnum << " is false" << endl;
}

void quickSort(int64_t *A, int left, int right)
{
	if (left >= right) return;
	int64_t x = A[(left + right) >> 1];
	int low = left, high = right;
	while (low<high)
	{
		while (A[low]<x)
			low++;
		while (A[high]>x)
			high--;
		if (low <= high)
		{
			int64_t Temp = A[low];
			A[low] = A[high];
			A[high] = Temp;
			low++;
			high--;
		}
	}
	quickSort(A, left, high);
	quickSort(A, low, right);
}

//���ڲ�����Ԫ����block�ĺ��������룺temp��size��block�������num_blockt
void judge1(int64_t *temp, int size, int64_t *block, int *&num_blockt)
{
	int begint = 0;
	for (int j = 0, k = 0; (j < comm_sz - 1) && (k < size);)
	{
		if (temp[j] < block[k])
		{
			j++;//������һ����Ԫ�ж�
			begint = begint + num_blockt[j - 1];
			if (j != comm_sz - 1)
				num_blockt[j] = 0;//��һ����ԪƬ�γ��ȳ�ʼΪ0
			else if (k != size)//��Ԫ��ͷ��blockδ��ͷ
				num_blockt[j] = size - begint;
		}
		else
		{
			num_blockt[j]++;
			k++;
			if ((k == size) && (j != comm_sz - 1))
			{
				while (j != comm_sz - 1)
					num_blockt[++j] = 0;
			}
		}
	}
}

//���ڲ��еĴ���judge1���������룺blockSize��temp��block�������num_block
void judge2(int blockSize, int64_t *temp, int64_t *block, int *&num_block)
{
	int size = blockSize / PNUM;
	int last = blockSize - (PNUM - 1)*size;
#pragma omp parallel for num_threads(PNUM)
	for (int i = 0; i < PNUM; i++)
	{
		int begint = 0;
		int *num_blockt = new int[comm_sz];
		num_blockt[0] = 0;
		if (i != PNUM - 1)
			judge1(temp, size, &block[i*size], num_blockt);
		else
			judge1(temp, last, &block[i*size], num_blockt);

		//���м����num_blockt��ֵ����num_block
		for (int j = 0; j < comm_sz; j++)
		{
#pragma omp atomic
			num_block[j] += num_blockt[j];
		}
	}
}

void PSRS(int64_t* block, int64_t*& data)
{
	double begin_time = 0, end_time = 0;


	begin_time = MPI_Wtime();

	int64_t *temp = new int64_t[comm_sz*comm_sz];//��p���������зֱ������p������
	int diff = blockSize / (comm_sz + 1);//ȡ��Ԫʱ�ļ��

	quickSort(block, 0, blockSize - 1);//���ڿ���

	int64_t *tempt = new int64_t[comm_sz];

	for (int i = 0; i < comm_sz; i++)
		tempt[i] = block[(i + 1)*diff];
	MPI_Gather(tempt, comm_sz, MPI_INT64_T, temp, comm_sz, MPI_INT64_T, 0, MPI_COMM_WORLD);
	if (my_rank == 0)
	{
		quickSort(temp, 0, comm_sz*comm_sz - 1);
		for (int i = 1; i < comm_sz; i++)//ѡȡp-1����Ԫ
			temp[i - 1] = temp[i*(comm_sz - 1)];//temp��0��comm_sz-2��comm_sz-1����Ч��Ԫ
												//store(temp, "block��Ԫ0.txt", comm_sz - 1);
	}
	MPI_Bcast(temp, comm_sz - 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

	//ȫ�ֽ���˼·���洢ÿһ�ε����ݸ���

	int *num_block = new int[comm_sz];
	int *begin = new int[comm_sz];
	for (int i = 0; i < comm_sz; i++)
	{
		num_block[i] = 0;
		begin[i] = 0;
	}

	judge2(blockSize, temp, block, num_block);

	for (int i = 1; i < comm_sz; i++)
		begin[i] = begin[i - 1] + num_block[i - 1];

	//num������comm_sz�����̵ģ�Ҫ���յ�������������num[2]�ǽ���2������������
	int *num = new int[comm_sz];
	MPI_Alltoall(num_block, 1, MPI_INT, num, 1, MPI_INT, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);

	//total���ý���Ҫ���յ��������������㴴����̬����
	int total = 0;
	for (int i = 0; i < comm_sz; i++)
		total += num[i];

	//new_block���ý��̽��յ�����������
	int64_t *new_block = new int64_t[total];
	int *recvDisp = new int[comm_sz];
	recvDisp[0] = 0;

	//recvDisp���ý���ÿ�ν�������ʱ��ƫ����
	for (int i = 1; i < comm_sz; i++)
		recvDisp[i] = num[i - 1] + recvDisp[i - 1];

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Alltoallv(block, num_block, begin, MPI_INT64_T, new_block, num, recvDisp, MPI_INT64_T, MPI_COMM_WORLD);

	quickSort(new_block, 0, total - 1);

	MPI_Gather(&total, 1, MPI_INT, num, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);

	if (my_rank == 0)
	{
		recvDisp[0] = 0;
		for (int i = 1; i < comm_sz; i++)
			recvDisp[i] = num[i - 1] + recvDisp[i - 1];
	}

	//�������ռ���������
	MPI_Barrier(MPI_COMM_WORLD);
	//MPI_Gatherv(new_block, total, MPI_INT64_T, data, num, recvDisp, MPI_INT64_T, 0, MPI_COMM_WORLD);

	mark = (int*)malloc(sizeof(int)*total);
	mark[0] = 0;
	for (int i = 1; i < total; i++) {
		if (new_block[i] != new_block[i - 1]) {
			new_block[i] = 1;
		}
		else {
			new_block[i] = 0;
		}
	}
	if (my_rank == 0) {
		mark_collection = new int[totalSize];
	}
	MPI_Gatherv(mark, total, MPI_INT, mark_collection, num, recvDisp, MPI_INT, 0, MPI_COMM_WORLD);

	printf("rank:%d ǰ׺���ռ���������", my_rank);

	if (my_rank == 0)
	{
		for (int i = 1; i < totalSize; i++) {
			mark_collection[i] = mark_collection[i] + mark_collection[i - 1];
		}
		cout << "***********������Ϊ��%d************" << mark_collection[totalSize - 1];
	}

	end_time = MPI_Wtime();
	/*if (my_rank == 0)
	store(data, "total.txt", BLOCK*LOOP);*/

}

//���������������PNUM���ļ���NAME��ѭ������LOOP
int main(int argc, char* argv[])
{
	//·��
	if (argc != 5)
	{
		cout << "������Ŀ����" << endl;
		return 0;
	}
	PNUM = atoi(argv[1]); //�߳���
	char* path = argv[2]; //�ļ�·��
	LOOP = atoi(argv[3]); //��С

	mark = (int*)malloc(totalSize * sizeof(int));//�洢��־�õ�

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	totalSize = BLOCK*LOOP;
	blockSize = totalSize / comm_sz;

	char a[10];
	int64_t *block = NULL;
	int64_t *data = NULL;
	data = (int64_t*)malloc(sizeof(int64_t)*totalSize);
	block = (int64_t *)malloc(sizeof(int64_t)*blockSize);
	readData(path, block);

	if (comm_sz == 1)
	{
		double begin_time, end_time;
		begin_time = MPI_Wtime();
		quickSort(block, 0, blockSize - 1);
		end_time = MPI_Wtime();
		cout << "time: " << end_time - begin_time << endl;
		cout << "data last: " << block[blockSize - 1] << endl;
	}
	else
	{
		PSRS(block, data);
	}

	//if(my_rank==0)
	delete[] data;
	delete[] block;
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
