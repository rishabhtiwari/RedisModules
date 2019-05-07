#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct item{
	long long width;
	char *adId;
	struct item *next;
};

void swap (char *x, char *y) {
	char temp = *x;
	*x = *y;
	*y = temp;
}

char* reverse(char *buffer, int i, int j) {
	while (i < j)
		swap(&buffer[i++], &buffer[j--]);

	printf("this is reverse%s\n", buffer);
	return buffer;
}
 
char* convertToCharArray(int n) {
	char *buf = (char*)malloc (4 * sizeof(char));
	sprintf(buf,"%d", n);
	return buf;
}

int main () {
	struct item *first[10];
	first [0] = (struct item*) malloc(sizeof(struct item));
	first[0] -> width = 10;
	first[0] -> adId = (char*) malloc(sizeof(char));
	strcpy (first[0] -> adId, "rishabh");
	printf("this is main%s\n", convertToCharArray(1213));
	first[0] -> adId = strcat (first[0] -> adId, convertToCharArray(121));
	first[0] -> next = (struct item*) malloc(sizeof(struct item));
	first[0] -> next -> width = 1;
	first[0] -> adId = (char*) malloc(sizeof(char));
	// strcpy (first[0] -> next -> adId, "rishabh");
	printf("%lld\n", first[0] -> next -> width);
	first[0] -> next -> next = NULL;
	printf("%s\n", "ok");
	struct item *temp = first[0] -> next;
	// printf("%lld %s\n", temp -> width , temp -> adId);
	free (first[0]);
	// printf("%lld %s\n", temp -> width , temp -> adId);
	free (temp);
	// printf("%lld %s\n", first[0] -> width , first[0] -> adId);

	int *a = (int*) malloc(10 * sizeof(int));
	a[0] = 1;
	a[1] = 2;
	a[2] = sizeof(NULL);
	printf("%d %d %d\n", a[0], a[1], a[2]);

}