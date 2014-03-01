{
  N=split($8,NN,"/")
  YY=NN[3]
  MM=NN[4]
  DD=NN[5]
  HH=NN[6]
  if(N == 6) print "alter table ais add if not exists partition (year=" YY ",month=" MM ",day=" DD ",hour=" HH ") location '" $8 "';"
}
