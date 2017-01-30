// Copyright (c) 2017, IBM Corporation <opendev@us.ibm.com>
//
// Permission to use, copy, modify, and/or distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

#ifndef SRC_UTIL_H_
#define SRC_UTIL_H_

#include <stddef.h>  // size_t
#include <stdlib.h>  // abort()

#if defined(__has_builtin) && __has_builtin(__builtin_unreachable)
# define UNREACHABLE() __builtin_unreachable()
#else
# define UNREACHABLE() abort()
#endif

namespace {

// The helper is for doing safe downcasts from base types to derived types.
template <typename Inner, typename Outer>
class ContainerOfHelper {
 public:
  inline ContainerOfHelper(Inner Outer::*field, Inner* pointer)
      : pointer_(reinterpret_cast<Outer*>(
            reinterpret_cast<uintptr_t>(pointer) -
            reinterpret_cast<uintptr_t>(&(static_cast<Outer*>(0)->*field)))) {}

  template <typename TypeName>
  inline operator TypeName*() const { return static_cast<TypeName*>(pointer_); }

 private:
  Outer* const pointer_;
};

// Calculate the address of the outer (i.e. embedding) struct from
// the interior pointer to a data member.
template <typename Inner, typename Outer>
inline ContainerOfHelper<Inner, Outer> ContainerOf(Inner Outer::*field,
                                                   Inner* pointer) {
  return ContainerOfHelper<Inner, Outer>(field, pointer);
}

template <typename T, size_t N>
inline size_t arraysize(const T (&)[N]) {
  return N;
}

}  // namespace anonymous

#endif  // SRC_UTIL_H_
