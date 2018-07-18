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
#define iterator_time 4
#define cash_size 512
int PNUM = 0;
int LOOP = 0;
char* NAME = NULL;

int comm_sz;
int my_rank;
int64_t totalSize;
int partialSize;
int blockSize;
int *mark;
int *mark_collection;
int iterator1;

void readData(string path, int64_t *block, int size, int64_t offset)
{
	ifstream file(path.data(), ios_base::binary);
	file.seekg(offset * sizeof(int64_t));
	file.read((char*)block, size * sizeof(int64_t));
	file.close();
}

void storeData(string path,int64_t *data,int size,int64_t offset){
	ofstream out(path.data(),ios_base::binary);
	out.seekg(offset*sizeof(int64_t));
	out.write(data,size*sizeof(int64_t));
	out.close();
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

//用于参照主元划分block的函数，输入：temp，size，block，输出：num_blockt
void judge1(int64_t *temp, int size, int64_t *block, int *&num_blockt)
{
	int begint = 0;
	for (int j = 0, k = 0; (j < comm_sz - 1) && (k < size);)
	{
		if (temp[j] < block[k])
		{
			j++;//进入下一个主元判断
			begint = begint + num_blockt[j - 1];
			if (j != comm_sz - 1)
				num_blockt[j] = 0;//下一个主元片段长度初始为0
			else if (k != size)//主元到头，block未到头
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

//用于并行的处理，judge1的任务，输入：blockSize，temp，block，输出：num_block
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

		//将中间变量num_blockt的值赋给num_block
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

	int64_t *temp = new int64_t[comm_sz*comm_sz];//从p个处理器中分别提出的p个数据
	int diff = blockSize / (comm_sz + 1);//取主元时的间隔

	quickSort(block, 0, blockSize - 1);//块内快排

	int64_t *tempt = new int64_t[comm_sz];

	for (int i = 0; i < comm_sz; i++)
		tempt[i] = block[(i + 1)*diff];
	MPI_Gather(tempt, comm_sz, MPI_INT64_T, temp, comm_sz, MPI_INT64_T, 0, MPI_COMM_WORLD);
	if (my_rank == 0)
	{
		quickSort(temp, 0, comm_sz*comm_sz - 1);
		for (int i = 1; i < comm_sz; i++)//选取p-1个主元
			temp[i - 1] = temp[i*(comm_sz - 1)];//temp的0至comm_sz-2存comm_sz-1个有效主元
												//store(temp, "block主元0.txt", comm_sz - 1);
	}
	MPI_Bcast(temp, comm_sz - 1, MPI_INT64_T, 0, MPI_COMM_WORLD);

	//全局交换思路：存储每一段的数据个数

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

	//num是来自comm_sz个进程的，要接收的数据量，例：num[2]是进程2发来的数据量
	int *num = new int[comm_sz];
	MPI_Alltoall(num_block, 1, MPI_INT, num, 1, MPI_INT, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);

	//total：该进程要接收的数据总量，方便创建动态数组
	int total = 0;
	for (int i = 0; i < comm_sz; i++)
		total += num[i];

	//new_block：该进程接收到的所有数据
	int64_t *new_block = new int64_t[total];
	int *recvDisp = new int[comm_sz];
	recvDisp[0] = 0;

	//recvDisp：该进程每次接收数据时的偏移量
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

	//主进程收集最终数据
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Gatherv(new_block, total, MPI_INT64_T, data, num, recvDisp, MPI_INT64_T, 0, MPI_COMM_WORLD);

	// mark = (int*)malloc(sizeof(int)*total);
	// mark[0] = 0;
	// for (int i = 1; i < total; i++) {
	// 	if (new_block[i] != new_block[i - 1]) {
	// 		new_block[i] = 1;
	// 	}
	// 	else {
	// 		new_block[i] = 0;
	// 	}
	// }
	// if (my_rank == 0) {
	// 	mark_collection = new int[partialSize];
	// }
	// MPI_Gatherv(mark, total, MPI_INT, mark_collection, num, recvDisp, MPI_INT, 0, MPI_COMM_WORLD);

	// printf("rank:%d 前缀和收集到主进程", my_rank);

	// if (my_rank == 0)
	// {
	// 	for (int i = 1; i < partialSize; i++) {
	// 		mark_collection[i] = mark_collection[i] + mark_collection[i - 1];
	// 	}
	// 	cout << "***********最大分组为：%d************" << mark_collection[partialSize - 1];
	// }


	end_time = MPI_Wtime();
	if (my_rank == 0){
		char temp[10];
		sprintf(a,"%d",iterator1);
	    string ss;
		ss.append("partialData_");
		ss.append(a);
		ss.append(".txt");
		storeData(ss, data, BLOCK*LOOP,0);
	}	
}

//输入参数：进程数PNUM，文件名NAME，循环次数LOOP
//数据文件为：100G_File.txt
//数据文件太大了，考虑分四次去读
int main(int argc, char* argv[])
{
	//路径
	if (argc != 5)
	{
		cout << "参数数目不对" << endl;
		return 0;
	}
	PNUM = atoi(argv[1]); //线程数
	char* path = argv[2]; //文件
	LOOP = atoi(argv[3]); //要读的文件大小
	iterator1=0;

	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	totalSize=BLOCK*LOOP;
	partialSize = totalSize/iterator_time;
	blockSize = partialSize / comm_sz;
	
	int64_t *block = NULL;
	int64_t *data = NULL;
	//mark = (int*)malloc(partialSize * sizeof(int));//存储标志用的

	for(int i=0;i<iterator_time;i++){
		data = (int64_t*)malloc(sizeof(int64_t)*partialSize/iterator_time);
		block = (int64_t *)malloc(sizeof(int64_t)*blockSize);
	    readData(path, block,blockSize,my_rank*blockSize+partialSize*iterator1);

		if (comm_sz == 1)
		{
			double begin_time, end_time;
			begin_time = MPI_Wtime();
			quickSort(block, 0, blockSize - 1);
		}
		else
		{
			iterator1++;
			PSRS(block, data);
		}
		MPI_Barrier(MPI_COMM_WORLD);
		delete[] data;
		delete[] block;
	}
	//生成了iterator_num个partialData_.txt个临时文件
	//缩小成2个文件
	if(my_rank<(iterator_time/2)){
		int time=partialSize/cash_size;
		for(int i=0;i<time;i++){
				int64_t *Four2twoCash1=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK);
				int64_t *Four2twoCash2=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK);
				int64_t *Four2twoCash3=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK*2);
				int temp1=0,temp2=0,temp3=0;
				char a[10];
				sprintf(a,"%d",my_rank*2);
				string ss;
				ss.append("partialData_");
				ss.append(a);
				ss.append(".txt");
				readData(ss.data(),Four2twoCash1,cash_size*BLOCK,i*cash_size*BLOCK);

				char b[10];
				sprintf(b,"%d",my_rank*2+1);
				string ss1;
				ss1.append("partialData_");
				ss1.append(a);
				ss1.append(".txt");
				readData(ss1.data(),Four2twoCash2,cash_size*BLOCK,i*cash_size*BLOCK);

				//两个缓存区合并成一个
				for(int j=0;j<cash_size*BLOCK*2;j++){
					if(temp1==cash_size*BLOCK){
						if(temp2!=cash_size*BLOCK){
							for(int z=temp2;z<cash_size*BLOCK;z++){
								Four2twoCash3[i]=Four2twoCash2[z];
							}
							break;
						}else{
							break;
						}
					}
					if(temp2==cash_size*BLOCK){
						for(int z=temp1;z<cash_size*BLOCK;z++){
							Four2twoCash3[i]=Four2twoCash1[z];
						}
					}
					if(Four2twoCash1[temp1]<Four2twoCash2[temp2]){
						Four2twoCash3[i]=Four2twoCash1[temp1];
						temp1++;
					}else{
						Four2twoCash3[i]=Four2twoCash2[temp1];
						temp2++;
					}

				char c[10];
				sprintf(c,"%d",my_rank);
				string ss2;
				ss2.append("F2TData_");
				ss2.append(c);
				ss2.append(".txt");	
				storeData(ss2,Four2twoCash3,cash_size*BLOCK*2,i*cash_size*BLOCK*2);
				delete []Four2twoCash1;
				delete []Four2twoCash2;
				delete []Four2twoCash3;
		}
	}

	//缩小成1个文件
	
if(my_rank<(iterator_time/4)){
		int time=partialSize*2/cash_size;
		for(int i=0;i<time;i++){
				int64_t *Four2twoCash1=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK);
				int64_t *Four2twoCash2=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK);
				int64_t *Four2twoCash3=(int64_t*)malloc(sizeof(int64_t)*cash_size*BLOCK*2);
				int temp1=0,temp2=0,temp3=0;
				char a[10];
				sprintf(a,"%d",my_rank*2);
				string ss;
				ss.append("F2TData_");
				ss.append(a);
				ss.append(".txt");
				readData(ss.data(),Four2twoCash1,cash_size*BLOCK,i*cash_size*BLOCK);

				char b[10];
				sprintf(b,"%d",my_rank*2+1);
				string ss1;
				ss1.append("F2TData_");
				ss1.append(a);
				ss1.append(".txt");
				readData(ss1.data(),Four2twoCash2,cash_size*BLOCK,i*cash_size*BLOCK);

				//两个缓存区合并成一个
				for(int j=0;j<cash_size*BLOCK*2;j++){
					if(temp1==cash_size*BLOCK){
						if(temp2!=cash_size*BLOCK){
							for(int z=temp2;z<cash_size*BLOCK;z++){
								Four2twoCash3[i]=Four2twoCash2[z];
							}
							break;
						}else{
							break;
						}
					}
					if(temp2==cash_size*BLOCK){
						for(int z=temp1;z<cash_size*BLOCK;z++){
							Four2twoCash3[i]=Four2twoCash1[z];
						}
					}
					if(Four2twoCash1[temp1]<Four2twoCash2[temp2]){
						Four2twoCash3[i]=Four2twoCash1[temp1];
						temp1++;
					}else{
						Four2twoCash3[i]=Four2twoCash2[temp1];
						temp2++;
					}

				
				ss2.append("T2OData.txt");
				storeData(ss2,Four2twoCash3,cash_size*BLOCK*2,i*cash_size*BLOCK*2);
				delete []Four2twoCash1;
				delete []Four2twoCash2;
				delete []Four2twoCash3;
		}
	}

	//获得了一个顺序文件





	}
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();

	return 0;
}
