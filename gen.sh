#!/bin/sh

base_path=$(cd `dirname $0`; pwd)
cd $base_path

rm -rf api_go
mkdir api_go

api_path=$base_path/api
proto_dir=$api_path

function gengofile() {
	echo $proto_dir
	protoc \
    --go_out=api_go \
	--go-grpc_out=api_go \
	--proto_path=api \
	-I$base_path \
	-I$GOPATH/src/googleapis \
	$proto_dir/*.proto
}

function getdir(){
	gengofile $proto_dir
    for element in `ls $1`
    do  
        dir_or_file=$1"/"$element
        if [ -d $dir_or_file ]
        then 
			proto_dir=$dir_or_file
			gengofile $proto_dir
        fi  
    done
}

getdir $api_path
# go mod init
go mod tidy