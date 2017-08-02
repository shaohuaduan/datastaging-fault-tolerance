#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include "mpi.h"
#include "dataspaces.h"
#include "debug.h"

int32_t rankID;	

int main(int argc, char *argv[]){
	
	int32_t totalNumTasks;//MPI: Total Number of processes, ID for process
	int64_t w, h, num_w, num_h;	//Openslide: each tile width, hight, left_top coordinate

	uint64_t lb[2] = { 0 }, rt[2] = { 0 };	//DataSpaces: left_bottom, right_top coordinate
	uint32_t ver = 1;	//DataSpaces: version for data
	int32_t size = sizeof(uint32_t);	//DataSpaces: (in bytes) for each element of the global array
	int32_t ndim = 2;	//DataSpaces: the number of dimensions for the global array
	MPI_Comm gcomm;
    //printf("TEST: %s %s %s %s\n", argv[0], argv[1], argv[2], argv[3]);
	w = (int64_t)atoi(argv[2]);//obj width
	h = (int64_t)atoi(argv[3]);//obj hight
	
	num_w = (int64_t)atoi(argv[4]);//tile width 
	num_h = (int64_t)atoi(argv[5]);//tile hight

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rankID);
	MPI_Comm_size(MPI_COMM_WORLD, &totalNumTasks);
	uloga("tile width w= %"PRId64" tile hight h= %"PRId64" totalNumTasks= %d \n", w, h, totalNumTasks);
	//initialize dataSpaces for all
	gcomm = MPI_COMM_WORLD;
	dspaces_init(totalNumTasks, 1, &gcomm, NULL);
	
	//allocate temp buffer for image tile data
	int64_t num_bytes = w * h * sizeof(uint32_t);//image tile data size
	uint32_t *buf = (uint32_t *)malloc(num_bytes);

	//openslide read image tile data in seperate process
	uloga("my rankID = %d, sub-process partition num_h= %"PRId64" num_w= %"PRId64" \n", rankID, num_h, num_w);
	int32_t base_x = 0;
	int32_t base_y = rankID * num_h * h;
	int32_t i;
	int32_t j;
	for (i = 0; i < num_h; i++){	
		for (j = 0; j < num_w; j++){
			//printf("x: %"PRId64", y: %"PRId64", level: %d, w: %"PRId64", h: %"PRId64" \n", x, y, level, w, h);
			//write image tile data into DataSpaces
			lb[0] = j * w + base_x;
			lb[1] = i * h + base_y;
			rt[0] = (j + 1) * w - 1 + base_x;
			rt[1] = (i + 1) * h - 1 + base_y;
			printf("rankID = %d, case 6: dspaces_put( argv[1] = %s, ver = %"PRId32", size = %"PRId32", ndim = %"PRId32", lb[0] = %"PRId64", lb[1] = %"PRId64", rt[0] = %"PRId64", rt[1] = %"PRId64", buf = %"PRId64" )\n", rankID, argv[1], ver, size, ndim, lb[0], lb[1], rt[0], rt[1], sizeof(buf));
			dspaces_put(argv[1], ver, size, ndim, lb, rt, buf);
			//turn to write next image tile
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	
	sleep(10);
	//openslide read image tile data in seperate process
	uloga("my rankID = %d, sub-process partition num_h= %"PRId64" num_w= %"PRId64" \n", rankID, num_h, num_w);
	base_x = 0;
	base_y = rankID * num_h * h;
	for (i = 0; i < num_h; i++){
		for (j = 0; j < num_w; j++){
			//printf("x: %"PRId64", y: %"PRId64", level: %d, w: %"PRId64", h: %"PRId64" \n", x, y, level, w, h);
			//write image tile data into DataSpaces
			lb[0] = j * w + base_x;
			lb[1] = i * h + base_y;
			rt[0] = (j + 1) * w - 1 + base_x;
			rt[1] = (i + 1) * h - 1 + base_y;
			printf("rankID = %d, case 6: dspaces_put( argv[1] = %s, ver = %"PRId32", size = %"PRId32", ndim = %"PRId32", lb[0] = %"PRId64", lb[1] = %"PRId64", rt[0] = %"PRId64", rt[1] = %"PRId64", buf = %"PRId64" )\n", rankID, argv[1], ver, size, ndim, lb[0], lb[1], rt[0], rt[1], sizeof(buf));
			dspaces_put(argv[1], ver, size, ndim, lb, rt, buf);
			//turn to write next image tile
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	

	//test get data from DataSpaces
	//enum node_status { normal, initializing, failed };
	//dspaces_server_status_test(int peer_id, int status);
	printf("dspaces_server_status_test(1, 2); my rankID = %d \n", rankID);
	if (rankID == 0){
		//dspaces_server_status_test(1, 2);	
	}
	sleep(10);
	dataSpaces_get(argv[1], ver, size, ndim, w, h, num_w, num_h, buf);
	
	free(buf);
	
	dspaces_finalize();
	MPI_Finalize();
	return 0;
}
 
int dataSpaces_get(char * dsarray, uint32_t ver, int32_t size, int32_t ndim, int64_t w, int64_t h, int64_t num_w, int64_t num_h, uint32_t *buf){
	
	uint64_t lb[2] = { 0 }, rt[2] = { 0 };	//DataSpaces: left_bottom, right_top coordinate
	uint64_t pref_lb[2] = { 0 }, pref_rt[2] = { 0 };	//DataSpaces: left_bottom, right_top coordinate
	int32_t pref_size = 1;	//prefetching list size

	double beg_time = MPI_Wtime();	//record time begin

	//openslide read image tile data in seperate process
	printf("dataSpaces_get my rankID = %d, sub-process partition num_h= %"PRId64" num_w= %"PRId64" \n", rankID, num_h, num_w);
	int32_t base_x = 0;
	int32_t base_y = rankID * num_h * h;
	int32_t i;
	int32_t j;
	//num_h = 1;
	//num_w = 1;
	for (i = 0; i < num_h; i++){
		for (j = 0; j < num_w; j++){
			//printf("x: %"PRId64", y: %"PRId64", level: %d, w: %"PRId64", h: %"PRId64" \n", x, y, level, w, h);
			//openslide_read_region(osr, buf, x, y-1, level, w, h);
			//write image tile data into DataSpaces
			lb[0] = j * w + base_x;
			lb[1] = i * h + base_y;
			rt[0] = (j + 1) * w - 1 + base_x;
			rt[1] = (i + 1) * h - 1 + base_y;
			printf("rankID = %d, case 6: dspaces_get( dsarray = %s, ver = %"PRId32", size = %"PRId32", ndim = %"PRId32", lb[0] = %"PRId64", lb[1] = %"PRId64", rt[0] = %"PRId64", rt[1] = %"PRId64", buf = %"PRId64" )\n", rankID, dsarray, ver, size, ndim, lb[0], lb[1], rt[0], rt[1], sizeof(buf));
			dspaces_get(dsarray, ver, size, ndim, lb, rt, buf);
			//dspaces_put(dsarray, ver, size, ndim, lb, rt, buf);
			//turn to write next image tile
		}
	}
	double end_time = MPI_Wtime();	//record time end
	printf("dataSpaces_get: beg_time = %lf s, end_time = %lf s, total_time = %lf s\n", beg_time, end_time, end_time - beg_time);
}