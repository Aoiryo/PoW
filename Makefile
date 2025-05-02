PKGNAME = PoW
MKARGS = -timeout 600s

.PHONY: build all
.SILENT: build all

build:
	cd $(PKGNAME) && sh setup_env.sh
	go build -C $(PKGNAME)

all: build
	go test -C $(PKGNAME) -v $(MKARGS) --race

docs:
	cd $(PKGNAME) && go doc -all > pow-doc.txt
