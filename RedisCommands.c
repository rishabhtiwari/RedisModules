#include "../redismodule.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define REDISMIN_SUPPORTED_INTEGER 1000000000000
#define REDISMAX_SUPPORTED_INTEGER 1000000000000
#define MAXHEIGHT_DIGIT 4
#define MAXADID_LENGTH 7
#define ll long long

struct item{
	double width;
	char *adId;
	struct item *next;
};

char* convertToCharArray(int n) {
	char *buf = (char*)malloc (MAXHEIGHT_DIGIT * sizeof(char));
	sprintf(buf,"%d", n);
	return buf;
}

long long convertToInteger(const char *str) {
	if (str == NULL) return 0;
	else {
		long long  ans = 0;
		int i =0;
		while (i < MAXADID_LENGTH && str[i] != '\0' && str[i] == ' ') i++;
		while (i < MAXADID_LENGTH && str[i] != '\0') {
			if (str[i] >= '0' && str[i] <= '9') {
				ans = (str[i] - '0') + ans *10;
			}
			else return ans;
			i++;
		}
		return ans;
	}
}

long long convertRedisStringToInteger (const RedisModuleString *str) {
	size_t len;
	if (str == NULL) return 0;
	const char *param = RedisModule_StringPtrLen(str, &len);
	return convertToInteger(param);
}

void freeItemList (struct item *items) {
	struct item *tempItem;
	while (items != NULL) {
		tempItem = items;
		items = items -> next;
		free (tempItem);
	}
}

int helloworld (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	RedisModule_AutoMemory(ctx);
    RedisModule_ReplyWithLongLong(ctx,rand());
    return REDISMODULE_OK;
}

int hgetset (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if(argc != 4){
		return RedisModule_WrongArity(ctx);
	}
	RedisModule_AutoMemory(ctx);
	RedisModuleCallReply *rep = RedisModule_Call (ctx, "hget", "ss", argv[1], argv[2]);
	if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
		return  RedisModule_ReplyWithCallReply(ctx, rep);
	}
	RedisModuleCallReply *srep = RedisModule_Call (ctx, "hset", "sss", argv[1], argv[2], argv[3]);
	if(RedisModule_CallReplyType(srep) == REDISMODULE_REPLY_ERROR) {
		return RedisModule_ReplyWithCallReply(ctx, srep);
	}
	return RedisModule_ReplyWithCallReply(ctx, rep);
}

int lmax (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if(argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RedisModuleKey *sourceListKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
	int sourceListKeyType = RedisModule_KeyType(sourceListKey);
	if (sourceListKeyType != REDISMODULE_KEYTYPE_LIST && sourceListKeyType != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_CloseKey(sourceListKey);
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

	long long maxItem = -REDISMIN_SUPPORTED_INTEGER;
	size_t sourceListLength = RedisModule_ValueLength(sourceListKey);
	size_t j;
    long long val;
	for (j = 0; j < sourceListLength; j++) {
		RedisModuleString *ele = RedisModule_ListPop(sourceListKey, REDISMODULE_LIST_TAIL);
        RedisModule_ListPush(sourceListKey, REDISMODULE_LIST_HEAD, ele);
        if ((RedisModule_StringToLongLong(ele, &val) == REDISMODULE_OK)) {
        	if (maxItem < val) maxItem = val;
        }
	}
	RedisModule_ReplyWithLongLong(ctx,maxItem);
    return REDISMODULE_OK;
}

int lsum (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

    RedisModuleKey *sourceListKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int sourceListKeyType = RedisModule_KeyType(sourceListKey);

	if (sourceListKeyType != REDISMODULE_KEYTYPE_LIST && sourceListKeyType != REDISMODULE_KEYTYPE_EMPTY) {
		RedisModule_CloseKey(sourceListKey);
		return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
	}

    long long sum = 0;
    size_t sourceListLength = RedisModule_ValueLength(sourceListKey);
    size_t j;
    for (j = 0; j < sourceListLength; j++) {
        RedisModuleString *ele = RedisModule_ListPop(sourceListKey, REDISMODULE_LIST_TAIL);
        RedisModule_ListPush(sourceListKey, REDISMODULE_LIST_HEAD, ele);
        long long val;
        if ((RedisModule_StringToLongLong(ele, &val) == REDISMODULE_OK)) {
        	sum += val;
        }
    }
    RedisModule_ReplyWithLongLong(ctx,sum);
    return REDISMODULE_OK;
}

void printItemSet (RedisModuleCtx *ctx, struct item *head) {
	size_t len;
	while (head != NULL) {
		len = strlen (head -> adId);
		RedisModuleString *adId = RedisModule_CreateString(ctx, head -> adId, len);
		// RedisModule_ReplyWithString (ctx, adId);
		// arrayCnt ++;
		head = head -> next;
	}
}

int createRedisSortedSet (RedisModuleCtx *ctx, const char *keyName, size_t len, struct item *items) {
	size_t needUpdateScore = 1;
	RedisModuleString *key1 = RedisModule_CreateString(ctx, keyName, len);
    printItemSet(ctx, items);
	if (ctx != NULL && keyName != NULL && items != NULL) {
		RedisModuleString *key = RedisModule_CreateString(ctx, keyName, len);
		RedisModuleCallReply *rep = RedisModule_Call(ctx, "del", "s", key);
		if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
			return  RedisModule_ReplyWithCallReply(ctx, rep);
		}
		RedisModuleKey *sortedSetKey = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE);
		while (items != NULL) {
			RedisModuleString *member = RedisModule_CreateString(ctx, items -> adId, strlen(items -> adId));
			double prevMemberScore = -1;
			needUpdateScore = 1;
			if (RedisModule_ZsetScore(sortedSetKey, member, &prevMemberScore) == REDISMODULE_OK) {
				if (prevMemberScore <= items -> width) {
					needUpdateScore = 0;
				}
			}
			if (needUpdateScore > 0 && RedisModule_ZsetAdd(sortedSetKey, items -> width, member, NULL) == REDISMODULE_ERR){
				RedisModule_CloseKey(sortedSetKey);
				return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
			}
			items = items -> next;
		}
		RedisModule_CloseKey(sortedSetKey);
	}
	return 0;
}

struct item *mergeSet(RedisModuleCtx *ctx, int idx, struct item *lc, struct item *rc, const char *keyName) {
    char *currentNodeKey =  (char*) malloc(strlen(keyName) * sizeof(char));
	strcpy (currentNodeKey, keyName);
	currentNodeKey = strcat(currentNodeKey, convertToCharArray(idx));
	struct item *currentNodeItems = (struct item*) malloc(sizeof(struct item));
	currentNodeItems -> next = NULL;
	struct item *currentNodeItemsHeadPtr = currentNodeItems, *tempItem, *tempItem2;
	// while (lc != NULL && rc != NULL) {
	// 	if (lc -> width < rc -> width) {
	// 		tempItem = currentNodeItems -> next = (struct item*) malloc(sizeof(struct item));
	// 		tempItem -> width = rc -> width;
	// 		tempItem -> adId = rc -> adId;
	// 		tempItem -> next = NULL;
	// 		tempItem2 = rc; 
	// 		rc = rc -> next;
	// 		currentNodeItems = tempItem;
	// 		free (tempItem2);
	// 	}
	// 	else {
	// 		tempItem = currentNodeItems -> next = (struct item*) malloc(sizeof(struct item));
	// 		tempItem -> width = lc -> width;
	// 		tempItem -> adId = lc -> adId;
	// 		tempItem -> next = NULL;
	// 		tempItem2 = lc; 
	// 		lc = lc -> next;
	// 		currentNodeItems = tempItem;
	// 		free (tempItem2);
	// 	}
	// }
	while (lc != NULL) {
		tempItem = currentNodeItems -> next = (struct item*) malloc(sizeof(struct item));
		tempItem -> width = lc -> width;
		tempItem -> adId = lc -> adId;
		tempItem -> next = NULL;
		tempItem2 = lc; 
		lc = lc -> next;
		currentNodeItems = tempItem;
		free (tempItem2);
	}
	while (rc != NULL) {
		tempItem = currentNodeItems -> next = (struct item*) malloc(sizeof(struct item));
		tempItem -> width = rc -> width;
		tempItem -> adId = rc -> adId;
		tempItem -> next = NULL;
		tempItem2 = rc; 
		rc = rc -> next;
		currentNodeItems = tempItem;
		free (tempItem2);
	}
	createRedisSortedSet(ctx, currentNodeKey, strlen(currentNodeKey), currentNodeItemsHeadPtr -> next);
	return currentNodeItemsHeadPtr -> next;
}

struct item *buildSegTree (RedisModuleCtx *ctx, int idx , int hmin ,int hmax, struct item **itemsHeadPtr, const char *keyName){
	if ( hmin == hmax ) { 
	    char *currentNodeKey =  (char*) malloc(strlen(keyName) * sizeof(char));
		strcpy (currentNodeKey, keyName);
		currentNodeKey = strcat(currentNodeKey, convertToCharArray(idx));
    	createRedisSortedSet(ctx, currentNodeKey, strlen(currentNodeKey), itemsHeadPtr[hmin]);
    	return itemsHeadPtr[hmin];
    }
    else {
    	int mid = (hmin + hmax) >>1 ;
        struct item *leftChildSetItems = buildSegTree (ctx, idx <<1 , hmin , mid, itemsHeadPtr, keyName) ;
        struct item *rightChildSetItems = buildSegTree (ctx, idx <<1|1 , mid +1 , hmax, itemsHeadPtr, keyName) ;
        return mergeSet (ctx, idx , leftChildSetItems, rightChildSetItems, keyName) ;
    }
}

int buildSegTreeInRedis (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	if (argc < 4 || ((argc-4)%3) != 0) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	long long hmin;
	long long hmax;
	const char *keyName;
	size_t len;
	keyName = RedisModule_StringPtrLen (argv[1], &len);
	RedisModule_StringToLongLong(argv[2],&hmin);
	RedisModule_StringToLongLong(argv[3],&hmax);
	size_t totalItems = argc;
	size_t i;
	struct item *firstItemsPtr[hmax+1], *tempItem, *rearItemsPtr[hmax+1];
	for (i =0; i < hmax+1; i++) firstItemsPtr[i] = tempItem = rearItemsPtr[i] = NULL;
	for (i = 4; i < totalItems; i+=3) {
		long long height;
		size_t strlen;
		double width;
		long long adId;
		RedisModule_StringToLongLong(argv[i], &height);
		if (firstItemsPtr[height] == NULL) {
			firstItemsPtr[height] = (struct item*) malloc(sizeof(struct item));
			RedisModule_StringToDouble(argv[i+1], &(firstItemsPtr[height] -> width));
			firstItemsPtr[height] -> adId = RedisModule_StringPtrLen(argv[i+2], &strlen);
			rearItemsPtr[height] = firstItemsPtr[height];
			rearItemsPtr[height] -> next = NULL;
		}
		else {
			tempItem = rearItemsPtr[height] -> next = (struct item*) malloc(sizeof(struct item));
			RedisModule_StringToDouble(argv[i+1], &(tempItem -> width));
			tempItem -> adId = RedisModule_StringPtrLen(argv[i+2], &strlen);
			tempItem -> next = NULL;
			rearItemsPtr[height] = tempItem;
		}
	}
	struct item *items = buildSegTree(ctx, 1, hmin, hmax, firstItemsPtr, keyName);
	freeItemList (items);

	//saving metaData for segTree
	const char *tempKeyName = (char*) malloc (sizeof(char));
	strcpy(tempKeyName, keyName);
	const char *minHeightKey = strcat (keyName, ":metaDataMinHeight");
	RedisModuleCallReply *rep = RedisModule_Call (ctx, "set", "cl", minHeightKey, hmin);
	if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
		return  RedisModule_ReplyWithCallReply(ctx, rep);
	}
	strcpy (keyName, tempKeyName);
	const char *maxHeightKey = strcat (keyName, ":metaDataMaxHeight");
	RedisModuleCallReply *srep = RedisModule_Call (ctx, "set", "cl", maxHeightKey, hmax);
	if(RedisModule_CallReplyType(srep) == REDISMODULE_REPLY_ERROR){
		return  RedisModule_ReplyWithCallReply(ctx, srep);
	}
	RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

struct item *getAdsFromSortedSetKey(RedisModuleCtx *ctx, const char *keyName, ll WminReq) {
	size_t len = strlen (keyName), i = 0;
	RedisModuleString *sortedSetKey = RedisModule_CreateString(ctx, keyName, len);
    //need to change
    struct item *adIdListHead = NULL, *temp;
    adIdListHead = (struct item *) malloc(sizeof(struct item));
    adIdListHead -> next = NULL;
    temp = adIdListHead;
    RedisModuleKey *key = RedisModule_OpenKey(ctx,sortedSetKey,REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET) {
        // RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    	RedisModule_CloseKey(key);
        return NULL;
    }
    if (RedisModule_ZsetFirstInScoreRange(key, WminReq , REDISMAX_SUPPORTED_INTEGER, 0, 0) != REDISMODULE_OK) {
        // RedisModule_ReplyWithError(ctx,"invalid range");
    	RedisModule_CloseKey(key);
        return NULL;
    }
    while(!RedisModule_ZsetRangeEndReached(key)) {
        double score = -1;
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key, &score);
        temp = temp -> next = (struct item *) malloc(sizeof(struct item));
        temp -> adId = (char *) malloc(sizeof(char));
        temp -> width = score;
        temp -> next = NULL;
        strcpy(temp -> adId, RedisModule_StringPtrLen(ele, &len));
        RedisModule_ZsetRangeNext(key);
    }
    RedisModule_ZsetRangeStop(key);
    RedisModule_CloseKey(key);
	if (adIdListHead) return adIdListHead -> next;
	else return NULL;
}

//need to free
struct item *mergeResult (struct item *lc , struct item *rc) {
	struct item *mergedList, *temp, *nxt;
	mergedList = (struct item *) malloc(sizeof(struct item));
	mergedList -> next = NULL;
	temp = mergedList;
	while (lc != NULL && rc != NULL) {
		if ((lc -> width) < (rc -> width)) {
			temp = temp -> next = (struct item *) malloc(sizeof(struct item));
			temp -> adId = (char *) malloc(sizeof(char));
			strcpy (temp -> adId, lc -> adId);
			temp -> width = lc -> width;
			temp -> next = NULL;
			nxt = lc;
			lc = lc -> next;
			free(nxt);
		}
		else {
			temp = temp -> next = (struct item *) malloc(sizeof(struct item));
			temp -> adId = (char *) malloc(sizeof(char));
			strcpy (temp -> adId, rc -> adId);
			temp -> width = rc -> width;
			temp -> next = NULL;
			nxt = rc;
			rc = rc -> next;
			free(nxt);
		}
	}
	while (lc != NULL) {
		temp = temp -> next = (struct item *) malloc(sizeof(struct item));
		temp -> adId = (char *) malloc(sizeof(char));
		strcpy (temp -> adId, lc -> adId);
		temp -> width = lc -> width;
		temp -> next = NULL;
		nxt = lc;
		lc = lc -> next;
		free(nxt);
	}
	while (rc != NULL) {
		temp = temp -> next = (struct item *) malloc(sizeof(struct item));
		temp -> adId = (char *) malloc(sizeof(char));
		strcpy (temp -> adId, rc -> adId);
		temp -> width = rc -> width;
		temp -> next = NULL;
		nxt = rc;
		rc = rc -> next;
		free(nxt);
	}
	if (mergedList) return mergedList -> next;
	else return NULL;
}

struct item *adsQuery (RedisModuleCtx *ctx, int idx, ll hmin, ll hmax, ll hminReq, ll hmaxReq, ll WminReq, const char* keyName) {

	if ( hmin > hmaxReq || hmax < hminReq ) return NULL;
	else if (hminReq <= hmin  &&  hmaxReq >= hmax  ) {
		char *currentNodeKey =  (char*) malloc(strlen(keyName) * sizeof(char));
		strcpy (currentNodeKey, keyName);
		currentNodeKey = strcat(currentNodeKey, convertToCharArray(idx));
    	return getAdsFromSortedSetKey (ctx, currentNodeKey, WminReq);
    }
    else {
        int mid = ( hmin + hmax ) >>1 ;
        struct item *leftChildAdIds = adsQuery (ctx, idx <<1 ,hmin, mid, hminReq ,hmaxReq, WminReq, keyName);
        struct item *rightChildAdIds = adsQuery (ctx, idx <<1|1 , mid+1, hmax, hminReq ,hmaxReq, WminReq, keyName);
        return mergeResult (leftChildAdIds, rightChildAdIds);
    }
}

// only min and exact supported;
int getAds (RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
	int arrayCnt = 0;
	if (argc != 5 && argc != 6) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	const char *keyName;
	size_t len;
	const RedisModuleString *hmin1, *hmax1;
	long long  hmin, hmax;
	keyName = RedisModule_StringPtrLen (argv[1], &len);
	if (keyName == NULL) {
		return RedisModule_ReplyWithError (ctx, "Err Invalid keyName");
	}
	const char *tempKeyName = (char*) malloc (len * sizeof(char));
	strcpy(tempKeyName, keyName);
	const char *minHeightKey = strcat (keyName, ":metaDataMinHeight");
	RedisModuleCallReply *rep = RedisModule_Call (ctx, "get", "c", minHeightKey);
	if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
		return RedisModule_ReplyWithError (ctx, "Err min height Key Not Exist");
	}
	const RedisModuleString *hminStr = RedisModule_CreateStringFromCallReply (rep);
	if (hminStr == NULL || RedisModule_StringToLongLong(hminStr, &hmin) != REDISMODULE_OK) {
		return RedisModule_ReplyWithError (ctx, "Err Invalid hmin Param");
	}
	// hmin = convertRedisStringToInteger(hminStr);
	strcpy (keyName, tempKeyName);
	const char *maxHeightKey = strcat (keyName, ":metaDataMaxHeight");
	RedisModuleCallReply *srep = RedisModule_Call (ctx, "get", "c", maxHeightKey);
	if(RedisModule_CallReplyType(srep) == REDISMODULE_REPLY_ERROR){
		return RedisModule_ReplyWithError (ctx, "Err max height Key Not Exist");
	}
	const RedisModuleString *hmaxStr = RedisModule_CreateStringFromCallReply (srep);
	if (hmaxStr == NULL || RedisModule_StringToLongLong(hmaxStr, &hmax) != REDISMODULE_OK) {
		return RedisModule_ReplyWithError (ctx, "Err Invalid hmax Param");
	}
	// hmax = convertRedisStringToInteger(hmaxStr);
	long long l, r, limit;
	strcpy (keyName, tempKeyName);

	// l = convertRedisStringToInteger (argv[2]);
	// r = convertRedisStringToInteger (argv[3]);
	// limit = convertRedisStringToInteger (argv[4]);
	if (argv[2] == NULL || argv[2] == NULL || argv[2] == NULL || RedisModule_StringToLongLong (argv[2], &l) != REDISMODULE_OK || RedisModule_StringToLongLong (argv[3], &r) != REDISMODULE_OK || RedisModule_StringToLongLong (argv[4], &limit) != REDISMODULE_OK || limit < -1 || l < 0 || r < 0) {
		return RedisModule_ReplyWithError (ctx, "Err Invalid Params");
	}

	RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
	RedisModule_ReplyWithLongLong (ctx, hmin);
	RedisModule_ReplyWithLongLong (ctx, hmax);
	RedisModule_ReplyWithLongLong (ctx, l);
	RedisModule_ReplyWithLongLong (ctx, r);
	RedisModule_ReplyWithLongLong (ctx, limit);
	RedisModule_ReplyWithLongLong (ctx, argc);

	char *finalAdsSetKey = "finalAdsSetKey";
	struct item *adIdList = adsQuery (ctx, 1, hmin, hmax, l, hmax, r, keyName);
	size_t i = 0;
	RedisModuleCallReply *dRep;
	if(argc == 6 && argv[5] != NULL) dRep = RedisModule_Call(ctx, "del", "s", argv[5]);
	else dRep = RedisModule_Call(ctx, "del", "c", finalAdsSetKey);
	if(RedisModule_CallReplyType(dRep) == REDISMODULE_REPLY_ERROR){
		RedisModule_ReplyWithError(ctx, "Error Deleting Previous Key");
		arrayCnt ++;
		RedisModule_ReplySetArrayLength(ctx, arrayCnt);
		return REDISMODULE_OK;
	}
	arrayCnt += 6;
	while (adIdList != NULL && (i < limit || limit == -1)) {
		RedisModule_ReplyWithLongLong (ctx, convertToInteger(adIdList -> adId));
		RedisModule_ReplyWithDouble (ctx, adIdList -> width);
		arrayCnt += 2;
		RedisModuleCallReply *rep;
		if(argc == 6 && argv[5] != NULL) rep = RedisModule_Call(ctx, "sadd", "sc", argv[5], adIdList -> adId);
		else rep = RedisModule_Call(ctx, "sadd", "cc", finalAdsSetKey, adIdList -> adId);
		if(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR){
			RedisModule_ReplyWithError(ctx, "Error Adding member in set");
			arrayCnt ++;
			RedisModule_ReplySetArrayLength(ctx, arrayCnt);
			return REDISMODULE_OK;
		}
		adIdList = adIdList -> next;
		i++;
	}
	freeItemList(adIdList);
	if(argc == 6 && argv[5] != NULL) RedisModule_ReplyWithString(ctx, argv[5]);
	else RedisModule_ReplyWithString(ctx, RedisModule_CreateString(ctx, finalAdsSetKey, strlen(finalAdsSetKey)));
	arrayCnt ++;
	RedisModule_ReplySetArrayLength(ctx, arrayCnt);
	return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"redis",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.rand", helloworld, "write", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.hgetset", hgetset, "write", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.lmax", lmax, "readonly", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.lsum", lsum, "readonly", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.seg", buildSegTreeInRedis, "write", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    if(RedisModule_CreateCommand(ctx, "redis.segQuery", getAds, "write", 1, 1, 1) == REDISMODULE_ERR)
    	return REDISMODULE_ERR;
    return REDISMODULE_OK;
}