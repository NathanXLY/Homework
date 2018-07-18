#include "mpi.h" 
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <omp.h>
#include <time.h>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>

#define threads_count 4
#define BLOCK 1024*1024
#define INT_MAX 2147483647
using namespace std;

int cmp(const void* a, const void* b)
{
	if ((int64_t*)a - (int64_t*)b > 0) {
		return 1;
	}
	else
	{
		return -1;
	}
}
void PSRS(int64_t * a, int n, int rank, int64_t **out)
{
	FILE *fp;
	int base = n / threads_count;
	int64_t p[threads_count*threads_count] = { 0.0 };
	int64_t *p_main = new int64_t[threads_count - 1];
	int count[threads_count][threads_count] = { 0 };    //每个处理器每段的个数
	int *gap = new int[threads_count];
	int gap_thread[threads_count][threads_count] = { 0 };
	int gap_thread_new[threads_count][threads_count] = { 0 };
	int partitionSize[threads_count] = { 0 }; //新的处理空间大小								  
	int64_t *pivot_array = new int64_t[n];//处理器数组空间
										
#pragma omp parallel num_threads(threads_count)
	{
		int id = omp_get_thread_num();
		int local_num = base;
		if (id == threads_count - 1) {
			local_num = n - id * base;
		}
		int64_t *local_a = a + id * base;
		FILE *local_file;
		//局部排序
		sort(a + id * base, a + id * base + local_num);

		/*string file = std::to_string(id) + "_a.txt";
		char *filename = new char[file.length() + 1];
		strcpy(filename, file.c_str());
		local_file = fopen(filename, "w");
		for (int i = 0; i <base; i++) {
		fprintf(local_file, "%d ", a[id*base+i]);
		}
		fclose(local_file);*/

#pragma omp barrier
		for (int i = 0; i < threads_count; i++) {
			p[id*threads_count + i] = a[id*base + i * (base / threads_count)];
		}//正则采样

#pragma omp barrier
#pragma omp master
		{
			sort(p, p + threads_count * threads_count);
			for (int i = 0; i < threads_count - 1; i++) {
				p_main[i] = p[(i + 1)*threads_count];
				//printf("rank:%d--p_main:%d\n", rank, p_main[i]);
			}
		}
#pragma omp barrier
		//数出每个线程该处理多少个数据
		int index = 0;
		for (int i = 0; i < local_num; ) {
			if (a[id*base + i] > p_main[index]) {
				index++;
			}
			if (index == threads_count - 1) {
				count[id][index] = local_num - i;
				break;
			}
			if (a[id*base + i] <= p_main[index]) {
				count[id][index]++;
			}
			else {
				continue;
			}
			i++;
		}
		/*
		string file = std::to_string(rank)+"--"+::to_string(id) + "_count.txt";
		char *filename = new char[file.length() + 1];
		strcpy(filename, file.c_str());
		local_file = fopen(filename, "w");
		for (int i = 0; i <threads_count; i++) {
		fprintf(local_file, "%d ", count[id][i]);
		}
		fclose(local_file);
		*/
#pragma omp barrier
		for (int i = 1; i < threads_count; i++) {
			gap_thread[id][i] = gap_thread[id][i - 1] + count[id][i - 1];
		}


#pragma omp barrier
		//算出全局交换后每个线程该处理多少数据
		for (int i = 0; i < threads_count; i++) {
			partitionSize[id] += count[i][id];
		}
#pragma omp barrier
		for (int i = 1; i < threads_count; i++) {
			gap_thread_new[id][i] = gap_thread_new[id][i - 1] + count[i - 1][id];
		}
#pragma omp master
		{
			//算出每个线程处理的初始下标
			gap[0] = 0;
			for (int k = 1; k < threads_count; k++) {
				gap[k] = gap[k - 1] + partitionSize[k - 1];
			}
		}
#pragma omp barrier
		//全局交换
		for (int i = 0; i < threads_count; i++) {
			memcpy(pivot_array + gap[id] + gap_thread_new[id][i], a + i * base + gap_thread[i][id], count[i][id] * sizeof(int64_t));
		}

		//归并排序
#pragma omp barrier
		sort(&(pivot_array[gap[id]]), &(pivot_array[gap[id] + partitionSize[id]]));
		//printf("rank:%d--id:%d completed:\n", rank, id);
#pragma omp barrier		
#pragma omp master
		{
			/*int int_temp = rank;
			stringstream stream;
			stream << int_temp;
			string file = stream.str() + "_sortdata.txt";
			char *filename = new char[file.length() + 1];
			strcpy(filename, file.c_str());
			fp = fopen(filename, "w");
			for (int i = 0; i <n; i++) {
			fprintf(fp, "%d ", pivot_array[i]);
			}
			fclose(fp);
			printf("rank:%d--id:%d sortdata store completed:\n", rank, id);*/
			*out = pivot_array;
		}
	}
}
void merge(int *a, int l, int m, int r) {
	int n1 = m - l + 1;
	int n2 = r - m;
	int *L, *R;
	L = (int*)malloc((n1 + 1) * sizeof(int));
	R = (int*)malloc((n2 + 1) * sizeof(int));
	int i, j, k;

	for (i = 0; i < n1; i++)
		L[i] = a[l + i];
	L[i] = INT_MAX;
	for (j = 0; j < n2; j++)
		R[j] = a[m + j + 1];
	R[j] = INT_MAX;

	for (i = 0, j = 0, k = m; k <= r; k++) {
		if (L[i] <= R[j]) {
			a[k] = L[i];
			i++;
		}
		else {
			a[k] = R[j];
			j++;
		}
	}
	delete[]L;
	delete[]R;
}

void MergeSort(int *a, int l, int r) {
	if (l < r) {
		int mid = (l + r) / 2;
		MergeSort(a, l, mid);
		MergeSort(a, mid + 1, r);
		merge(a, l, mid, r);
	}
}
template<typename E>
void quickSort(E *A, int left, int right)
{
	if (left >= right) return;
	int x = A[(left + right) >> 1], low = left, high = right;
	while (low<high)
	{
		while (A[low]<x)
			low++;
		while (A[high]>x)
			high--;
		if (low <= high)
		{
			int Temp = A[low];
			A[low] = A[high];
			A[high] = Temp;
			low++;
			high--;
		}
	}
	quickSort(A, left, high);
	quickSort(A, low, right);
}


//第一步：局部排序&选取样本
void sort_and_pick_pivots(int64_t *local_data, int local_n, int64_t *pivots_local, int rank, int size) {
	//sort(local_data, local_data + local_n);//局部排序
	//quickSort<int64_t>(local_data, 0, local_n - 1);
	for (int i = 0; i < 10; i++) {
		printf("rank%d: local_data[%d]: %d", rank, i, local_data[i]);
	}
	qsort(local_data, local_n, sizeof(int64_t), cmp);
	printf("rank%d: 局部排序完成\n", rank);

	for (int i = 10; i < size; i++) {
		pivots_local[i] = local_data[i*int(local_n / size)];
		printf("rank%d: pivots_local[%d]:%d\n", rank, i, pivots_local[i]);
	}

	return;
}
//第二步：样本排序,选择主元
void sort_sample(int64_t *local_data, int local_n, int64_t *pivots_local, int *partitionSize, int rank, int size) {
	int64_t *collectedPivots = (int64_t *)malloc(sizeof(int64_t)*size*size);
	int64_t *pivots = (int64_t *)malloc(sizeof(int64_t)*size - 1);
	MPI_Gather(pivots_local, size, MPI_INT64_T, collectedPivots, size, MPI_INT64_T, 0, MPI_COMM_WORLD);
	//0号进程负责对主元进行排序
	if (rank == 0) {
		sort(collectedPivots, collectedPivots + size * size);

		for (int i = 0; i < size*size; i++) {
			printf("collectedPivots[%d]:%d \n", i, collectedPivots[i]);
		}

		for (int i = 0; i < size - 1; i++) {
			pivots[i] = collectedPivots[(i + 1)*size];
			printf("main_pivots[%d]:%d", i, pivots[i]);
		}
	}
	//把选出的主元的广播给所有进程
	MPI_Bcast(pivots, size - 1, MPI_INT64_T, 0, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);

	//测试广播 //成功
	for (int i = 0; i < size - 1; i++) {
		printf("rank:%d 主元 : pivots[%d]=%d\n", rank, i, pivots[i]);
	}

	for (int i = 0; i < 10; i++) {
		printf("step2:rank%d: local_data[%d]: %d \n", rank, i, local_data[i]);
	}

	//long index = 0L;
	//for (long i = 0; i < local_n; ) {
	//	if (local_data[i] - pivots[index]>0.000001) {
	//		index++;
	//	}
	//	if (index == size - 1) {
	//		partitionSize[index] = local_n - i; //说明后面的都是属于最后一个区域的
	//		break;
	//	}
	//	if (local_data[i] - pivots[index]<0.000001) {
	//		partitionSize[index]++;
	//	}
	//	else {
	//		continue;
	//	}
	//	i++;
	//}


	for (int i = 0; i < local_n; i++) {
		int j = 0;
		for (j = 0; j < size - 1; j++) {
			if (local_data[i] > pivots[j]) {}
			else {
				break;
			}
		}
		partitionSize[j]++;
	}
	free(collectedPivots);
	free(pivots);

	for (int i = 0; i < size; i++) {
		printf("rank%d: partitionSize[%d]:%d\n", rank, i, partitionSize[i]);
	}
	return;
}
//第三步：全局交换
int64_t * global_swap(int64_t *local_data, int local_n, int *partitionSize, int *newPartitionSize, int size, int rank, int64_t *totalSize_p) {
	int64_t totalSize = 0;
	int i;
	int64_t* newPartition;
	//第i个进程发送的第j块数据将被第j个进程接收并存放在其接收消息 缓冲区recvbuf的第i块。
	MPI_Alltoall(partitionSize, 1, MPI_INT, newPartitionSize, 1, MPI_INT, MPI_COMM_WORLD);

	/*for (int i = 0; i < size; i++) {
	cout << "step3.1: " << rank << ":  " << newPartitionSize[i] << "\t";
	}*/

	for (i = 0; i < size; i++) {
		totalSize += newPartitionSize[i];
	}
	*totalSize_p = totalSize;
	newPartition = (int64_t*)malloc(sizeof(int64_t)*totalSize);

	int *sendDisp = (int *)malloc(size * sizeof(int));
	int *recvDisp = (int *)malloc(size * sizeof(int));
	//在发送划分之前计算相对于sendbuf的位移，此位移处存放着输出到进程的数据
	sendDisp[0] = 0;
	//计算相对于recvbuf的位移，此位移处存放着从进程接受到的数据
	recvDisp[0] = 0;
	for (i = 1; i < size; i++) {
		sendDisp[i] = partitionSize[i - 1] + sendDisp[i - 1];
		recvDisp[i] = newPartitionSize[i - 1] + recvDisp[i - 1];
	}
	/*
	MPI_ALLTOALLV(sendbuf, sendcounts, sdispls, sendtype, recvbuf,
	recvcounts, rdispls, recvtype, comm)
	IN  sendbuf     发送消息缓冲区的起始地址(可变)
	IN  sendcounts  长度为组大小的整型数组, 存放着发送给每个进程的数据
	个数
	IN  sdispls     长度为组大小的整型数组,每个入口j存放着相对于sendbuf
	的位移,此位移处存放着输出到进程j的数据
	IN  sendtype    发送消息缓冲区中的数据类型(句柄)
	OUT recvbuf     接收消息缓冲区的起始地址(可变)
	IN  recvcounts  长度为组大小的整型数组, 存放着从每个进程中接收的元
	素个数(整型)
	IN  rdispls     长度为组大小的整型数组,每个入口i存放着相对于recvbuf
	的位移,此位移处存放着从进程i接收的数据
	IN  recvtype    接收消息缓冲区的数据类型(句柄)
	IN  comm        通信子(句柄)
	*/
	for (int i = 0; i < size; i++) {
		printf("rank%d:sendDisp[%d]:%d\n", rank, i, sendDisp[i]);
	}
	printf("rank:%d:  totalSize:%d\n", rank, totalSize);
	MPI_Alltoallv(local_data, partitionSize, sendDisp, MPI_INT64_T, newPartition, newPartitionSize, recvDisp, MPI_INT64_T, MPI_COMM_WORLD);
	/*for (int i = 0; i < size; i++) {
	cout << "step3.2: " << rank << ":  " << newPartition[i] << "\t";
	}*/
	free(sendDisp);
	free(recvDisp);
	return newPartition;
}
//第四步：归并排序
void merge_sort(int *newPartition, int totalSize, int rank) {
	if (newPartition == NULL) return;
	sort(newPartition, newPartition + totalSize);
	/*for (int i = totalSize - 1; i > totalSize - 20; i--) {
	cout << "step4.1: " << rank << ":  " << newPartition[i];
	}*/
	return;
}

int main(int argc, char* argv[])
{
	int64_t *data = NULL;//要排序的数据
	int64_t *local_data = NULL;//每个线程处理的局部数据
	int loop;
	int rank;//进程id
	int size;//进程数量
	long long local_n;//每个线程处理的局部数据大小
	int64_t *pivots_local;//局部主元
	int *partitionSize;//根据主元决定该分到每个进程多少数据
	int *newPartitionSize = NULL;//全局交换后每个进程该处理多少数据
	int64_t *newPartition = NULL;//全局交换后每个进程处理的数据
	int64_t *newSortPartition = NULL;//每个进程排完序之后的数据
	int64_t *index_collection = NULL;
	int64_t totalSize = 0;
	int writeData = 0;//判断是否写入数据
	FILE *fp;

	loop = atoi(argv[1]);
	writeData = atoi(argv[3]);
	long long data_size = BLOCK * loop;
	MPI_Init(NULL, NULL);//MPI环境初始化
	MPI_Comm_rank(MPI_COMM_WORLD, &rank); //获取当前进程号
	MPI_Comm_size(MPI_COMM_WORLD, &size); //获取进程总数
	double start, finish;

	//int64_t **newSortPartition_collection = new int64_t *[data_size];//每个进程排完序之后的数据
	int *totalSize_collection = new int[size];
	int *recvDisp = new int[size];
	MPI_Barrier(MPI_COMM_WORLD);
	printf("rank:%d 分配内存\n", rank);
	pivots_local = (int64_t *)malloc(sizeof(int64_t)*size);
	partitionSize = (int *)malloc(sizeof(int)*size);
	newPartitionSize = (int *)malloc(sizeof(int)*size);

	for (int i = 0; i < size; i++) {
		partitionSize[i] = 0;
	}

	//printf("rank:%d scatter\n", rank);
	//if (rank == 0) {
	//	data = (int *)malloc(sizeof(int)*data_size);
	//	//printf("rank:%d 数据分配空间\n", rank);
	//	//fp = fopen(argv[2], "r"); //以读方式新建一个文件
	//	//printf("rank:%d 打开文件\n", rank);
	//	//for (int i = 0; i<loop; i++)
	//	//{
	//	//	for (int j = 0; j < BLOCK; j++)
	//	//	{
	//	//		fscanf(fp, "%d", data + i*BLOCK + j);
	//	//	}
	//	//}
	//	//fclose(fp);

	//	ifstream file(argv[2], ios_base::binary);
	//	file.read((char *)data, sizeof(int)*data_size);

	//	printf("rank:%d 读完数据\n", rank);

	//	//for (int i = 0; i < 10; i++) cout << rank << ":   " << local_data[i]	 << "\t";

	//}

	int64_t blockSize = data_size / size;
	int64_t lastSize = data_size - ((size - 1)*blockSize);


	ifstream file(argv[2], ios_base::binary);
	printf("rank:%d blocksize:%d\n", rank, blockSize);

	if (rank != size - 1)
	{
		file.seekg(rank*blockSize * sizeof(int64_t));
		local_n = blockSize;
		local_data = (int64_t *)malloc(sizeof(int64_t)*local_n);
		printf("rank:%d 本地数据分配空间\n", rank);
		file.read((char*)local_data, local_n * sizeof(int64_t));
		printf("rank:%d 读完数据\n", rank);
	}
	else
	{
		file.seekg(rank*blockSize * sizeof(int64_t));
		local_n = lastSize;
		local_data = (int64_t *)malloc(sizeof(int64_t)*local_n);
		printf("rank:%d 本地数据分配空间\n", rank);
		file.read((char*)local_data, local_n * sizeof(int64_t));
		printf("rank:%d 读完数据\n", rank);
	}
	file.close();

	MPI_Barrier(MPI_COMM_WORLD);


	start = MPI_Wtime();

	if (size == 1) {

		sort(local_data, local_data + local_n);
		newPartition = local_data;
		return 0;
	}
	else {
		//第一步：局部排序&选取样本
		cout << "process" << rank << "enter step 1 " << endl;
		sort_and_pick_pivots(local_data, local_n, pivots_local, rank, size);

		for (int i = 0; i < size; i++) {
			pivots_local[i] = local_data[i*int(local_n / size)];
			printf("rank%d: pivots_local[%d]:%d\n", rank, i, pivots_local[i]);
		}

		cout << "process" << rank << "step 1 completed!" << endl;

		//第二步：样本排序
		sort_sample(local_data, local_n, pivots_local, partitionSize, rank, size);

		/*for (int i = 0; i < size-1; i++) {
		printf("out____rank%d: partitionSize[%d]:%d\n", rank, i, partitionSize[i]);
		}*/

		//free(pivots_local);

		MPI_Barrier(MPI_COMM_WORLD);

		cout << "process" << rank << "step 2 completed!" << endl;
		//第三步：全局交换
		newPartition = global_swap(local_data, local_n, partitionSize, newPartitionSize, size, rank, &totalSize);

		for (int i = 0; i < 20; i++) {
			printf("after 3 rank%d: newPartition[%d]:%d\n", rank, i, newPartition[i]);
		}

		/*free(local_data);
		free(partitionSize);*/

		cout << "process" << rank << "step 3 completed!" << endl;
		//第四步：归并排序
		printf("rank%d: totalSize:%d\n", rank, totalSize);
		//merge_sort(newPartition, totalSize, rank);
		//(MPI_COMM_WORLD);
		MPI_Gather(&totalSize, 1, MPI_INT, totalSize_collection, 1, MPI_INT, 0, MPI_COMM_WORLD);
		//PSRS(newPartition, totalSize, rank, &newSortPartition);
		sort(newPartition, newPartition + totalSize);
		for (int i = 0; i < 20; i++) {
			printf("rank%d: newPartition[%d]:%d\n", rank, i, newPartition[i]);
		}
		cout << "process" << rank << "step 4 completed!" << endl;


		if (rank == 0)
		{
			//data = (int64_t*)malloc(sizeof(int64_t)*data_size);
			recvDisp[0] = 0;
			for (int i = 1; i < size; i++)
				recvDisp[i] = totalSize_collection[i - 1] + recvDisp[i - 1];
		}

		MPI_Barrier(MPI_COMM_WORLD);
	}
	/*MPI_Gatherv(newSortPartition, totalSize, MPI_INT64_T, data, totalSize_collection, recvDisp, MPI_INT64_T, 0, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);*/
	//free(newPartition);

	/*for (int i = 0; i < 20; i++) {
		printf("rank:%d newSortPartition[%d]:%d\n", rank, i, newSortPartition[i]);
	}*/
	/*for (int i = 0; i < 20; i++) {
		printf("rank:%d newPartition[%d]:%d\n", rank, i, newPartition[i]);
	}*/

	printf("rank:%d 开始做标志\n", rank);
	int64_t *index = (int64_t*)malloc(totalSize * sizeof(int64_t));
	index[0] = 0;
	for (int i = 1; i < totalSize; i++) {
		if (newPartition[i] != newPartition[i - 1]) {
			index[i] = 1;
		}
		else {
			index[i] = 0;
		}
	}
	printf("rank:%d 做完标志\n", rank);
	//得出每个进程的标志

	//获得前缀和
	printf("rank:%d 开始计算前缀和\n", rank);
	for (int i = 1; i < totalSize; i++) {
		index[i] = index[i] + index[i - 1];
	}
	printf("rank:%d 计算完前缀和\n", rank);

	if (rank == 0) {
		index_collection = new int64_t[data_size];
	}
	printf("rank:%d 计算完前缀和\n", rank);
	//把前缀和收集到主进程
	MPI_Gatherv(index, totalSize, MPI_INT64_T, index_collection, totalSize_collection, recvDisp, MPI_INT64_T, 0, MPI_COMM_WORLD);
	printf("rank:%d 前缀和收集到主进程", rank);
	MPI_Barrier(MPI_COMM_WORLD); 
	//分组号
	if (rank == 0) {
		for (int i = 1; i < data_size; i++) {
			index_collection[i] = index_collection[i] + index_collection[i - 1];
		}
		cout<<"***********最大分组为：%d************"<<index_collection[data_size-1];
	}


	finish = MPI_Wtime();
	MPI_Barrier(MPI_COMM_WORLD);


	/*int int_temp = rank;
	stringstream stream;
	stream << int_temp;
	string file = stream.str() + "_newsortdata.txt";
	char *filename = new char[file.length() + 1];
	strcpy(filename, file.c_str());
	fp = fopen(filename, "w");
	for (int i = 0; i <totalSize; i++) {
	fprintf(fp, "%d ", newSortPartition[i]);
	}
	fclose(fp);
	MPI_Barrier(MPI_COMM_WORLD);*/


	if (rank == 0) {

		if (writeData != 0) {
			cout << "准备写入数据";
			string file = "sortdata.txt";
			char *filename = new char[file.length() + 1];
			strcpy(filename, file.c_str());
			fp = fopen(filename, "w");
			for (int i = 0; i < BLOCK*loop; i++) {
				fprintf(fp, "%d ", data[i]);
			}
			fclose(fp);
			cout << "写入数据完成";
		}
		else {
			cout << "不写入数据";
		}

		cout << "completed!" << endl;
		cout << "mpi的计时函数：Elapsed time is " << finish - start << " seconds" << endl;
	}
	MPI_Barrier(MPI_COMM_WORLD);

	MPI_Finalize();

	return 0;
}
