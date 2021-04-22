#include <stdlib.h>
#include <stdio.h>
struct addr{
  char name[5];
  void* adr;
};
struct addr Adresses[5];

int cmp (const void* a, const void* b)
{
  struct addr* x = (struct addr*)a;
  struct addr* y = (struct addr*)b;
  return (x->adr - y->adr)>0?1:(x->adr - y->adr)<0?1:0;
}


int alpha = 59;
const char* lambda = "GAMMA";
int func (int var);

int main()
{
  //printf("MAIN adress = %016p\n", &main);
  Adresses[0] = (struct addr){"MAIN", &main};

  //printf("ADRESS OF GLOBAL: %016p\n", &alpha);
  Adresses[1] = (struct addr){"GLOBAL", &alpha};
  int betta = 24;
  //printf("ADRESS OF LOCAL: %016p\n", &betta);
  Adresses[2] = (struct addr){"LOCAL", &betta};
  //const char* gamma = "GAMMA";
  //printf("ADRESS OF const char: %016p\n", &lambda);
  Adresses[3] = (struct addr){"Const char", &lambda};

  int* sigma = (int*)calloc(10,sizeof(int));
  //printf("ADRESS OF ptr: %016p\n", &sigma);
  Adresses[4] = (struct addr){"PTR", &sigma};
  int i = 0;
  qsort(Adresses,5, sizeof(struct addr), cmp);
  for (i = 0 ; i < 5; i ++)
  {
    printf("%s: %p\n", Adresses[i].name, Adresses[i].adr);
  }
  free(sigma);
  func(1);
  return 0;
}

int func (int var)
{
  int FuncVar = 12312;
    printf("ADRESS OF LOCAL IN ANOTHER FU: %016p\n", &FuncVar);
    return -1;
}
