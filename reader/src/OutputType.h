
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/chunked_array.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <map>

class OutputType
{

public:

        // Returns index of community equals to name
        // Returns UNKNOWN if none
        static unsigned int match(std::string name) {
            for (unsigned int index = 0; index < COUNT; ++index) {
                if (strcmp(OUTPUT_TYPES[index], name.c_str()) == 0) {
                    return index;
                }
            }
            return UNKNOWN;
        }

        enum {
            CSV,
            FIFO,
            IPC,
            SOCKET,
            STDOUT,
            UNKNOWN,
            COUNT = UNKNOWN,
        };


private:


        static inline const char *OUTPUT_TYPES[] = {
                "csv",
                "fifo",
                "ipc",
                "socket",
                "stdout",
        };


};
