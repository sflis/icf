
#include "icf/icfFile.h"
#include "doctest.h"
#include <fstream>
#include <vector>
#include <iostream> 
#include <chrono>

namespace icf {


TEST_CASE("ICFFile") {
    icf::ICFFile icf_file(std::string("/tmp/test.icf"),ICFFile::trunc);
    std::vector<uint8_t> data = {1,2,3,4,2,3,4};
    auto n =3;
    for(uint16_t i = 0; i<n; i++ ){
        icf_file.write((void*)data.data(),data.size());    
    }
    auto time_original = icf_file.get_timestamp();

    SUBCASE("Consistency_while_writing") {
                        
            CHECK(icf_file.size() == n);

            auto read_data = icf_file.read_at(1);
            uint8_t *datar = (uint8_t*) read_data->data();
            for(uint16_t i =0; i<data.size(); i++){
                // std::cout<<data[i]<<" "<<datar[i]<<std::endl;
                CHECK(data[i] == datar[i]);
            }
    }

    icf_file.close();

    SUBCASE("Consistency_while_reading") {
        icf::ICFFile icf_file(std::string("/tmp/test.icf"));
        CHECK(icf_file.size() == n);

        auto read_data = icf_file.read_at(1);
        uint8_t *datar = (uint8_t*) read_data->data();
        for(uint16_t i =0; i<data.size(); i++){
            // std::cout<<data[i]<<" "<<datar[i]<<std::endl;
            CHECK(data[i] == datar[i]);
        }
        CHECK(time_original == icf_file.get_timestamp());        
    }

}

}  // namespace icf
