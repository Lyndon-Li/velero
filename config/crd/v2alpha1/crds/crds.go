/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by crds_generate.go; DO NOT EDIT.

package crds

import (
	"bytes"
	"compress/gzip"
	"io"

	apiextinstall "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

var rawCRDs = [][]byte{
	[]byte("\x1f\x8b\b\x00\x00\x00\x00\x00\x00\xff\xbcYKs\xe3\xb8\x11\xbe\xfbWtM\x0es\x19\xca;\xc9V*\xa5ی\x9cT\xb9\xb23\xa3Z9\xbe\x83D\x8b\xc2\x0e\b x\xc8q\x1e\xff=\xd5\x00I\x81$d[\xdady\x13\x1e\x1f>t7\xba?@UU\xdd0#\x1e\xd1:\xa1\xd5\x1a\x98\x11\xf8\x0f\x8f\x8a~\xb9\xd5\xf7?\xb9\x95зǏ7߅\xe2k\xd8\x04\xe7u\xf73:\x1dl\x83w\xb8\x17Jx\xa1\xd5M\x87\x9eq\xe6\xd9\xfa\x06\x80)\xa5=\xa3fG?\x01\x1a\xad\xbc\xd5R\xa2\xadZT\xab\xef\xa1\xc6:\b\xc9\xd1F\xf0a\xe9\xe3\x0f\xab\x8f?\xae~\xb8\x01P\xac\xc35\x10\x1e\xd7OJj\xc6\xdd\xea\x88\x12\xad^\t}\xe3\f6\x04\xdcZ\x1d\xcc\x1aN\x1dib\xbfh\"|\xc7<\xbb\xeb1b\xb3\x14\xce\xffu\xd1\xf5\x93p>v\x1b\x19,\x93\xb3\xb5c\x8f\x13\xaa\r\x92\xd9i\xdf\r\x80k\xb4\xc15|\xa5\xa5\rk\x90\xda\xfa=E*\x150Σ\x95\x98\xdcZ\xa1<ڍ\x96\xa1\x1b\xacS\x01G\xd7Xa|\xb4BN\v\x9cg>8p\xa19\x00s\xf0\x15\x9fn\xef\xd5\xd6\xea֢K\xb4\x00~qZm\x99?\xaca\x95\x86\xaf́9\xec{\x93)w\xb1\xa3o\xf2\xcf\xc4\xd7y+T[b\xf0 :\x04\x1elt!\xed\xbbA\xf0\a\xe1\xa6Ԟ\x98#z\xd6#?K$\xf6\x13\x9c\xf3\xac3sF\xd9\xd4D\x893\x8f%B\x1b\xdd\x19\x89\x1e9\xd4\xcf\x1e\x87m\xec\xb5\xed\x98_\x83P\xfe\x8f?\x9e\xb7Eo\xacU\x9cz\xa7\xd5\xd40\x9f\xa9\x15\xb2\xe6Ą\xbcԢ-ZG{&\x7f\r\x11O\x00\x9f\xb3\xf9\x89I\xc2\xcd\xdb_\xa5B!\az\x0f\xfe\x80\xf0\x995߃\x81\x9dז\xb5\b?\xe9&\xb9\xef\xe9\x80\x16\xe3\x88:\x8d\xa0\xe8\x05A\xbeӶ\xe8:\x83\xcd*\x8d\xed\xc1\x06\xac\x99\xff\xa6\v\xfd\xcfc\xab\xb1Ȋ\xb15\xa4\x9aU\x1c!\xb4*\aا\x16\xdf\x14\\\xb9\x11\x95\xe6\x98Yl\xc2I80V7\xe8\xdc\v\x01O\x00\x13\x16_O\r\vӤ\x11\xc7\xdf3i\x0e\xeccJ2\xcd\x01;\xb6\xeegh\x83\xea\xd3\xf6\xfe\xf1\x0f\xbbI3\xbc\x900X\xe3\x1de\n\xa2o\xac\xf6\xba\xd1\x12j\xf4O\x88*\xb9\xbe\xd3G\xb4\x94\xe7Z\xa1܈HY\x9b\xe7\x03N9\x9b\xe2;\xe2Qo\xea\xb4\x18\xa3\x87\b\xda\xdc\xfb@k\x1a\xb4^\fY\xb8\xc7>\x15\x98\xacu\xb6\x8f\x7fW\x93>\x00\xdaz\x9a\x05\x9c*\r\xa6m\xf5\xb9\x15yo\xad\xe4<\xe1\xc0\xa2\xb1\xe8P\xa5\xdaC\xcdL\x81\xae\x7f\xc1Ưf\xd0;\xb4\x04\x03\ue803\xe4\xb4\xd9#Z\x0f\x16\x1b\xdd*\xf1\xcf\x11ہ\xd7qQ\xc9<:\x1f\x0f\xa3UL\u0091ɀ\x1f\xc8h3\xe4\x8e=\x83EZ\x13\x82\xca\xf0\xe2\x047\xe7\xf1\x85\xac(\xd4^\xaf\xe1\xe0\xbdq\xeb\xdb\xdbV\xf8\xa1\xec6\xba\xeb\x82\x12\xfe\xf96zC\xd4\xc1k\xebn9\x1eQ\xde:\xd1V\xcc6\a\xe1\xb1\xf1\xc1\xe2-3\xa2\x8a\x1bQ\xb1\xf4\xae:\xfe;\xdb\x17j7Yv\x11\x88\xe9\x8b\x05\xf3\x02\xf7P\x15\xa5S\xc1z\xa8\xb4œ\x17\xa8\x89L\xf7\xf3\x9fw\x0f00I\x9eJN9\r]\xd8e\xf0\x0fYS\xa8=\xda4oou\x171Qq\xa3\x85\xf2\xf1G#\x05*\x0f.ԝ\xf0\x14\x06\x7f\x0f\xe8<\xb9n\x0e\xbb\x89\xd2\x04j\x84`(\x1f\xf0\xf9\x80{\x05\x1b֡\xdc0\x87\xbf\xb1\xaf\xc8+\xae\"'\xbc\xc9[\xb9\xe0\x9a\x0fN\xe6\xcd:\x06\xc5tƵy\x06\xd9\x19lȫdX\x9a&\xf6\xa2\xaf$\x94\x06\xd8d\xec\xd4B\xe5\xa3O_\xb1\x9a\xcc\a\xbd\x16n\xf4}.\x01\rlU\x96\xc8\xfbZ\xe7\xfa\"%\xa7E*\xff\x16\xf5Ѣ\xd1Nxm\x9fOUr\x1e\ng\xbdB_\xc3T\x83\xf2\x9a\xedm\xe2L\x10\x8a\x93\xcdq\feJB\t5\x12ժ\xd5t\xb8&\xae\x80{Oc(\xb6\x1d\xfa\xf2FU\xb1\xaa\t\x05'M\t\xb9v\x9co\xb7\xd6Z\"\x9b[\x91\xa2\xf0\v\x95\x85\x8dV{\xd1.7\x9e\xcb\xdfs!\xf2\x8aM\v\x01\x9b-I\xbb\xa0\xe8$&U\xacP\xd5\x10\xba\x94\xda\xf7\xa2\r\xf6\x9c\xff\xf7\x02%_䟳'i\xd8p\\\xe5\x1a\x1f\x8fԇ\xd3\xd5W\xb5\xac\xf4z\x1d3\x94\x8bz7\v\xcd%I\x80\xfb}\x86(\x1c\xbc{\a\xda»t'z\xf7!\xcd\x0eB\xfaJL\xea\xff\x93\x90rX\xe5\xa2\xe8&\x85\xf3m\xf7\xca\xceI\xf5\x10\x9bo\xbb\x17\x94\xd5\xdf\xccBW]\xc4d\x14\x1f\xa4\xfft\xf0\xd7x\xe3\xdb\fc\xe6\x14Oj5:\xc2kxb\"\x13\x00\xe3\xea\xeeC\x01\xb7\xc6=U\v\x8b>XEg\x15\xad\xa5\xf4\xe9\"\xa4\x0e\vA\xf2\xe2N\x9db\xc6\x1d\xb4\xbf\xbf{e\x8f\xbbq\xe0\x90\x14\xef\xef\x06\x0f<Ɛ\x183c?\x12\xbc.\xd2\x1f$\x1e\x8f5\xf72\xb6\xb1Џ\xd7\xe1kܲ\x9bB\f\x9b\xd1V\xb4\x82\x8c\xafƞSH\x1d\xe9N\x1d\x87\xd2\x16\x91C0g\xb8\x03\xe5JR\x165\x02\x17\xfb=Z\x92\x0fQ[\xa4\x85\xb7\x8f\x9b\xf7.[D\xec\xf3\x1f\x94\x96;f\fr\xbaȐs{[]d%\xcfl\x8b\xfe1\x92~\xc5D\x0f\xd9\xd0\xc1\x14\xa4C\xe8\xd6\xd9\v\xdd\x18\xacq\x18l\x1f7\x05YJ\xdf\xf6q\xc9\xf0|ц\xfe\x86rƉ\v\x96\vo\xf5|F\x8c\"ċ9\x1f\xc0\x1c߰\xf2\xf6\xb1$\x01Fs\x80?0O#\xfa\x1b%\xd4\xcfEL\x18\x8eH\xef\xce\xeb\xf86o\"\xbcy\x91\xf1fN\xf9\f\xdf\xfa\xf9WS&\x85!,\xf2%\xeb\xea\x05\xcfU`\x8e\xc5\xc6\xe6\xedu\xb4\xbcrU\x16\x8b\xb31\xf3\xd4?\xeb>\xe5\xcby\xc74\xaf\xccz\xf3#\xf9&U\x1d\xef\xfco\xd5\xd5\xe9%\xafw{\x13lL:\xfd\xfb\x1e]U\xafR\xd6Mz\x19\xcb\x1fA\xae\x12\x9eK\x98x\x17\xb6<\xab\x83l\xbc\xf6\xc7\xe7\x99\xe1M\xae\x94_Oxij̙\x04\x87\x1c\xf0\x88\n\xe8:\xc1\x84D>`\x16\x14\x18\xc0\x03\xddA\xe2M\xf0\xbd\x1b\x81b9&\xb5W \xed\x16 \xc3\xdb\x1c\xdd\xf6*\x82X\x8cPAJVK\\\x83\xb7\xe1\x9c\xf2-\x9e\x9e\x0e\x9dc\xedk\xd9\xfbK\x1a\x95n\xca\xfd\x14`5\x89\x8d\xb9\x1a\x7f\xef\xfa\x80\xb8X\x91\xbdQ\x8f\xa9+\u07ba.\xe2\x12o\x0f\xaf\x90\xd9Ҙ\xd2A\x18\xa9\x9d?\t\xf4\xa1\n])]}ŧB맦ASJ\xa1\x15l-\x1af\x8b]\x8bG\xf6\xbc3]\xd3J\xd9t\xe8+b\x8e\xaf\u0605\xbe\xbf\xc4\xc3p\x91\xa5{~\xd7\x1c\xf7\xf1\xb2w\xd0r8\xe1\xf1\xf5Y\x85\xaeFKn\x88\xefۃ?F\xb1\xc8\x14ϽVR\f#\xc2( #\xd4\n\x1e\x0eT\xcf\xd2\ru\x90\xd4\\8#\xd9\xf3\xb8\x99\\\xd6\x14\xc0O\xa7f\xf1\x00y\xa9\xb2\x19\xff\r(\x97\xebғ\xfe\xf4[>\xce\xcf\xfa\xc7W\xfe\xff\xcf\n/\xdcO\xa7\xff\xba\\\xa5\xbf'\b\xaf\x95\x82\xfe_\xa0\xcb3\xf8t\x99\xdf2y\x17\xad\xb7h\x8c\xccy\x86ݿ'\xe5-\xa1\x1e\x1fY\xd7\xf0\xaf\xff\xdc\xfc7\x00\x00\xff\xff\"\xf8F!P\x1d\x00\x00"),
	[]byte("\x1f\x8b\b\x00\x00\x00\x00\x00\x00\xff\xbcYIs\xe3\xb8\x15\xbe\xfbW\xbc\xea\x1c\xe6b\xc9\xd3\xc9T*\xa5[\xb7\x9cT\xb92\xedV\xb5\x1c\xdf!\xf2\x89\xc4\x18\x04\x18,R\x9c忧\x1e\x00R \t\xad3=<\xb8,,\x0fo\xc3\xf7\x16\xccf\xb3;\xd6\xf2WԆ+\xb9\x00\xd6r\xfc\x97EI\xbf\xcc\xfc\xed/f\xce\xd5\xc3\xee\xe3\xdd\x1b\x97\xe5\x02\x96\xceX\xd5|C\xa3\x9c.\xf0\x11\xb7\\r˕\xbckв\x92Y\xb6\xb8\x03`R*\xcbh\xd8\xd0O\x80BI\xab\x95\x10\xa8g\x15\xca\xf9\x9b\xdb\xe0\xc6qQ\xa2\xf6Ļ\xa3w?\xce?\xfe4\xff\xf1\x0e@\xb2\x06\x17@\xf4\\+\x14+\xcd|\x87\x02\xb5\x9asugZ,\x88l\xa5\x95k\x17p\x98\b\xdb⑁\xddGf\xd9?<\x05?(\xb8\xb1\x7f\x1fM\xfc̍\xf5\x93\xadp\x9a\x89\xc1\xa9~\xdcpY9\xc1t:s\a`\n\xd5\xe2\x02\x9e\xe9Ȗ\x15HcQ\x12\xcf\xc2\fXYz\xdd0\xb1\xd2\\Z\xd4K%\\\xd3\xe9d\x06%\x9aB\xf3\xd6z\xd9\x0f\f\x81\xb1\xcc:\x03\xc6\x1550\x03ϸ\x7fx\x92+\xad*\x8d&\xb0\x04\xf0\x8bQr\xc5l\xbd\x80yX>okf0\xce\x06\xf5\xad\xfdD\x1c\xb2\xefĭ\xb1\x9a\xcb*w\xfe\vo\x10J\xa7\xbd\xd9H\xe6\x02\xc1\xd6ܤ\x8c\xed\x99!\xe6\xb4\xc5\xf2(\x1b~\x9e\x88\x19˚v\xccO\xb250T2\x8b9v\x96\xaai\x05Z,a\xf3n\xb1\x13b\xabt\xc3\xec\x02\xb8\xb4\x7f\xfe\xe9\xb8&\xa2\xaa\xe6~룒C\xb5|\xa6QH\x86\x03'd\xa1\nuV7\xca2\xf1k\x18\xb1D\xe0s\xb2?p\x12\xe8\xa6\xe3gY!w\x03\xb5\x05[#|fśkam\x95f\x15\xc2Ϫ\b\xc6\xdbר\xa3\xf16a\x89\xa9\x95\x13%l:\x89\x01\x8cU:k\xc5\x16\x8by\xd8\x15\xe9vdG\xa6\x1c\x9e\xf9\x1b;Y\xa1\x91e\x9d\xacC\x99\xb9_\xc1\x95\xcc{ڧ\n/\xf2\xb2T\x9bR\x95ث\x0eS\x8e\xb8\x81V\xab\x02\x8d9\xe1\xf7\xb4}\xc0\xc3\xf3a`\xa2\x96\xb0b\xf7G&ښ}\f(S\xd4ذEܡZ\x94\x9fVO\xaf\x7fZ\x0f\x86\xe1(f\xb0\xc2\x1a\x02\vb\xbd\xd5ʪB\tؠ\xdd#J\x8f[Ш\x1dj\x02\xb9\x8aK\x03L\x96=MH\x17\x1c\xa0\x9a\x9c\xdcӣ\xd90\x19\xddI\xb5\xa8S\xb3\x03\x1d٢\xb6\xbcC\xdf\xf0%a%\x19\x1d\t\xf1\xdf\xd9`\x0e\x80\xe4\x0e\xbb\xa0\xa4\xf8\x82A\xaa\x88\xadXFU\x05\xbbq\x03\x1a[\x8d\x06e\x8884\xcc$\xa8\xcd/X\xd8\xf9\x88\xf4\x1a5\x91\xe9\xeeC\xa1\xe4\x0e\xb5\x05\x8d\x85\xaa$\xffwOۀU\xfeP\xc1,\x1a\xeb/\xa4\x96L\xc0\x8e\t\x87\xf7#\xed\xd1װw\xd0Hg\x82\x93\t=\xbf\xc1\x8c\xf9\xf8\xa24\x02\x97[\xb5\x80\xda\xda\xd6,\x1e\x1e*n\xbb`[\xa8\xa6q\x92\xdb\xf7\ao\f\xbeqVi\xf3P\xe2\x0eŃ\xe1Ռ\xe9\xa2\xe6\x16\v\xeb4>\xb0\x96ϼ \xd2\a\xdcyS\xfeA\xc7\xf0l\x06\xc7N\xbc0|>P^a\x1e\x8a\x9ft%X$\x15D<X\x81\x86Hu\xdf\xfe\xba~\x81\x8e\x93`\xa9`\x94\xc3҉^:\xfb\x906\xb9ܢ\x0e\xfb\xb6Z5\x9e&ʲU\\Z\xff\xa3\x10\x1c\xa5\x05\xe36\r\xb7\xe4\x06\xffth,\x99nLv\xe9\x13\x12\xd8 \xb8\x96\xa0\xa0\x1c/x\x92\xb0d\r\x8a%3\xf8;ۊ\xacbfd\x84\x8b\xac\x95\xa6Y\xe3\xc5A\xbd\xc9D\x97)\x1d1\xed\x01>\xd6-\x16dSR+m\xe2[\x1ec\ta\x00KV\x0e\xb5\x93\xbf\xf6\xf4eC\xc8x\xd19W\xa3\xefs\x8ePǫL\xf0\xbb\vu12\x89adJ\xbf\x03\xc8\xc7=\x1a[e\xb8U\xfa\x9d\b\x87\xd08v\x83\xa3\x16\xa1\xaf`\xb2@q\x8bxK\xbf\x13\xb8,I\xe3ػ1\x01P\xa0\xea\x19U\xb2Rt\xb1\x12C\xc0\x93\xa5\x15\xe4\xd5\x06m^L\x99\te\\\xc2!\x9b\x844k\x1c\x8b\xbaQJ \x1bk\xb00|-Ykje\xcf\b\xfc\xb4\x85n\xe5\xcb{\x8bt\xf8r\xfdtO\x7f\xbaq\xf2\xa0\x1d/#\xc4\xd3-\xa3\xbc*o\xb6h\xe7\xe5\xfa\tL\xdc>5\x92tB\xb0\x8d\xc0\x05X\xed\xa6\x82\x1dwX\xfa:\xb2K\xc1Lv\xc1H\xc0u\xba>\xe7\x93\x1dA(\xfc\n[\xb3\x9c\xa1\xbc\xc6)\xc2Qy\x90l\xe2}\"\x04{n\xeb\xec\xce\x13N\t1\xcdc\x15^,P\xb2<+O\xbc\\A\x1c\xb5=!\xcc\xeau\xe9\xe5='\x19a\xfb-\x92\x05\x92\xc7=q\"\xdb\xeb`CN\xba\x11\x97ǄSt\xe5\b9\xb0\x04\xd7^\xcf;\xddp\xae\xb1\x9c\xf2<\x1b\xd8+3=\x14\xfaȵ\x9d\x84\x01\x88\x19\xde\x17\xca\xe1\x96Jny5=;-VOݑ\x93\xa2M\xc2Kr$i\x9c\xa2\tq2\xf3\xe9\xe4\xac\v5\x94\x88my\xe5\xf4\xb1\xab\xbf\xe5(\xcaI\xb6p\xf6\xb6\x9fчg\xe2\x16\xd0\xee%\xeb\x82eį$\x8d\x0e^\xe2\x8c/`\x93X3\x95\x01\b'\x0f\x14\xb9\x81\x0f\x1f@i\xf8\x10\x1a\x1b\x1f\xee\xc3nǅ\x9d\xf1A.\xbf\xe7Bt\xa7\\\x15\xae\xfa\xfc\x9d\xaa'\xe5\xce\xe1xV\a_G4F\xaa\xb0T\xe9y\xf1\xad\x82=\xe3I\x0eݟn\xee3t7\xb8\xa5\x84K\xa3uZR\xc8C\xad)\a1\x9e\xa4r\x19\xcc?!\xa9I\xe2\xcf\x19)ǡ\xcaKA\xff\x8f\xb1<\x05\x80\x8c\x009\x1b\x9f\xe2\xd0\xe7\xc7}\x17\xe9\x16S\xac\x87$:\xe6\x95\xe6\x15'\x85\xcb~\xe6\x90\xf9D\xac\x8b-\x02\x8fd\x1e\x8a\xb3\xfe٣\xa5!\xb4<\x90\xa3\xeb\x1c\x0e'\xb4g\xb2\xf4\xc1\xb9\x9f/\xe3\xd5\xcb\\ܳ\nY\xbd.\xcf٫?8\x03\xe54\xbc\xafyQ\x0fMǧ\xa0\n`\xd9\x1b\xfaD\xf7\n6\xf3\x18>˧\xbd\xa35\xe3\xdb7\x9aN]v<54tvv\xf5\xba\xbc\xa84\xf0]\x8bˊ\x83Ў\x8cZ.\x9c־\xec\n\xa3Tm\xdfP\x1e\x14\xa1\xbd\x976pnʞ\xa7d|1\xaf\xcb\x04\x85X\x97\xe6\xfb\xceR\xd7W\xcc9\xfa\x81\\\xd8\xe9\x9b\vD\rK\xc0\x1dJ\xa0\x82\x88qA\xa0\xeeIf<\xfb4\x95\x88n\xa1\x89\xdcU\xca]W%۲\xa0\xef\x85|ؗ\xc6?\x98\x9e\xa6\aW\xba\x81\x19%LݼkXR\xf9;#\x12\xb7\xc5\xd2\xec\x8d\xeds\x8boh\x9c\xc8D\x93\xef\x98[\x84#C\xcdf\xb2\xb9\xc5颂\x19`\xa0\x03\x91\x88\x1dǼ\xf9b%e\x13\x8e\x06\x8da\xd59|\xff\x12V\x85\xfeJ\xdc\x02lC\xf1u\xc8\xda\x0f&\xde\xc0\xab\xb0U\xaa\xf2\x1c\aϪ\xf4\xc7˫[\xa3Ws\xf2u}!/_\xd7ߑ\x93\x96\xd9\xfa\f\x1f+f\xeb\x0e\xff\xb6N\b\xbfg\x92\x17Đ\xbaA\xba\u05ffUz\xe0\xeb\xf3s\xecњ\x1c>\xe3%.\x8d\xd25\xb9R\xe4\x19\xf7\x99\xd1OE\x81\xad\xcdH6\x83\x95Ɩ\xe9\xec\xd4\xe4\xf9*\x9d\f-\x90ܕ\xef\xe6\xb24\xfb\x17\xa2\xcc\xdc\xdf<H_\xa5\xe7\xc8\xdf-Q\xa8o\xa6\xd4Jt\x81ǿ\xecH\xd7lP\x93\x11\xfc\xdbѨΤ|)\xb1X\x86p\xb2\xbfO\xd2<\xa59\xbc\xd4TC\x87\xf6O\x97f\x97ܴ\x82\xbd\xf7\xb2\x9c\x83\xbe\x1eV\xc6m\xfd\xa9\x93\x9c\xee\x9b\xf4\xefl\xf92<\xf7X6\xfc\xa6\xcf^\xa3\xf9\xfe\xfd\xec\xfb\x9cp\x02\xb7\xbb\xeb\xfd\xf4xa\xfd\xf0\xf4\xd8]E^\xa2\xb4T\x12\x1d\x9eR\x0e\x99\xa8o\xcd\xe5t9nI^\x97<\x0f^_o*&\x06\x14\xcedS\xf118\x97\xb3\xac\t\f\b\x82|\xf3~9~\xae\xbb\xef_\xff\x98\x8d/\bE\xcdd\x85\xb9\f]I\n\xd1>\xc4_\x9f\x1e\r\x05\xfa=3\xa3\xacWM\x06=\xe7eB;\xf6\x80\xd2\x11\xb7\xe9\x9ft\x16\xf0\x9f\xff\xdd\xfd?\x00\x00\xff\xff\xea{\x80Q\xb4!\x00\x00"),
}

var CRDs = crds()

func crds() []*apiextv1.CustomResourceDefinition {
	apiextinstall.Install(scheme.Scheme)
	decode := scheme.Codecs.UniversalDeserializer().Decode
	var objs []*apiextv1.CustomResourceDefinition
	for _, crd := range rawCRDs {
		gzr, err := gzip.NewReader(bytes.NewReader(crd))
		if err != nil {
			panic(err)
		}
		bytes, err := io.ReadAll(gzr)
		if err != nil {
			panic(err)
		}
		gzr.Close()

		obj, _, err := decode(bytes, nil, nil)
		if err != nil {
			panic(err)
		}
		objs = append(objs, obj.(*apiextv1.CustomResourceDefinition))
	}
	return objs
}
