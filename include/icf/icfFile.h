#ifndef ICF_FILE_H__
#define ICF_FILE_H__

#include <cstdio>
#include <stdint.h>
#include <string>
#include <cstring>
#include <cassert>
#include <algorithm>

#include <vector>
#include <deque>
#include <list>
#include <set>
#include <map>

#include <exception>
#include <stdexcept>
#include <memory>

#include <fstream>
#include "icf/archive.h"

namespace icf{

class ICFFile{
    struct ICFFileHeader{

        ICFFileHeader(){
            strncpy(file_identifier,"ICF",4);
        }

        template <class T>
        void Serialize(T& archive)
        {
            archive & file_identifier;
        }

        char file_identifier[4];
    };

public:
    enum access_mode{read, trunc, append};

    ICFFile(std::string path, ICFFile::access_mode mode=ICFFile::append);

    ~ICFFile();

    void write(const void* data, std::size_t size);

    std::shared_ptr<std::vector<unsigned char> > read_at(uint64_t index);

    void flush();

    void close();

    ICFFileHeader file_header_;
private:

    std::fstream file_handle_;
    uint64_t current_read_pointer_;
    uint64_t current_write_pointer_;
    uint64_t data_start_point_;
    std::vector<uint64_t> object_index_;
    Archive<std::fstream> serializer_stream_;
};




}//icf namespace


#endif