      subroutine standardise(ns,nsgc,nycur,ngcur,nvar,iband,
     1                       itime,ngfut,nsgf,leap,idays,
     2                       nout,miss,phlwr,phupr,thres,
     3                       ilimit,rec,gcmc,gcmf,nvarmax,
     4                       nyrmax,monmax,ndmax,nsmax,nnmax)
ccccccccccccccccccccccccccc

c	Start year of observed data                 :ns+1
c	Number of years of observed data            :nycur
c	Start year of cur climate GCM data          :nsgc+1
c	Number of years of cur climate GCM data     :ngcur
c	Start year of fut climate GCM data          :nsgf+1
c	Number of years of fut climate GCM data     :ngfut


c	Width of moving window         : iband
c	Number of variables considered :nvar
c	Lower and upper limit on the variables: phll(),phul()
c	LEAP- decides about the leap year option; if 0, no leap year, all years have 365 days
c                                                if 1, leap years are considered, some years will have 366 days 
c	miss - decides on the missing value if less than -9000, values is considered as missing

ccccccccccccccccccccccccccc

        
        integer ns, nsgc, nycur, ngcur, nvar, iband, itime
        integer ngfut, nsgf
        integer nday(monmax), leap(4), idays(monmax,4)
        integer nvarmax, nyrmax, monmax, ndmax, nsmax, nnmax
        integer ilimit(nvarmax)
        
        real*4 gcmc(nvarmax,nyrmax,monmax,ndmax)
        real*4 gcmf(nvarmax,nyrmax,monmax,ndmax)
        real*4 rec(nvarmax,nyrmax,monmax,ndmax)
        real*4 avdh(nvarmax,monmax,ndmax),sddh(nvarmax,monmax,ndmax)
        real*4 avmc(nvarmax,monmax),avmh(nvarmax,monmax),
     1       sdmc(nvarmax,monmax),sdmh(nvarmax,monmax)
        real*4 a(nvarmax),avr(nvarmax),avh(nvarmax)
        real*4 gd(nvarmax,nyrmax,monmax,ndmax)
        real*4 ggd(nvarmax,nyrmax,monmax,ndmax)
        real*4 gdct(nvarmax,nyrmax,monmax,ndmax)
        real*4 avm(nvarmax,monmax),sdm(nvarmax,monmax)
        real*4 cordh(nvarmax,monmax,ndmax),cormh(nvarmax,monmax),
     1       cordc(nvarmax,monmax,ndmax)
        real*4 cormc(nvarmax,monmax),cord(nvarmax,monmax,ndmax),
     1       corm(nvarmax,monmax)

        real*4 gcm(nvarmax,nyrmax,monmax,ndmax)

        real*4 rem(nvarmax,nyrmax,monmax)

        real*4 gcur(nvarmax)
        real*4 gm(nvarmax,nyrmax,monmax),avyc(nvarmax),
     1       sdyc(nvarmax),coryc(nvarmax)
        real*4 ggm(nvarmax,nyrmax,monmax)
        real*4 avd(nvarmax,monmax,ndmax),
     1       sdd(nvarmax,monmax,ndmax),gmct(nvarmax,nyrmax,monmax)
        real*4 cory(nvarmax),avy(nvarmax),sdy(nvarmax),
     1       rey(nvarmax,nyrmax),gy(nvarmax,nyrmax),
     1       gyct(nvarmax,nyrmax),avyh(nvarmax),sdyh(nvarmax),
     1       coryh(nvarmax),ggy(nvarmax,nyrmax)

        real*4 crsmd(monmax,ndmax,nvarmax,nvarmax)
        real*4 crsod(monmax,ndmax,nvarmax,nvarmax)
        real*4 crsmm(monmax,nvarmax,nvarmax)
        real*4 crsom(monmax,nvarmax,nvarmax)
        real*4 crsmy(nvarmax,nvarmax)
        real*4 crsoy(nvarmax,nvarmax)
        real*4 cmod(monmax,ndmax,nvarmax,nvarmax)
        real*4 gmod(monmax,ndmax,nvarmax,nvarmax)
        real*4 cobs(monmax,ndmax,nvarmax,nvarmax)
        real*4 gobs(monmax,ndmax,nvarmax,nvarmax)

        real*4 cmodm(monmax,nvarmax,nvarmax)
        real*4 gmodm(monmax,nvarmax,nvarmax)
        real*4 cobsm(monmax,nvarmax,nvarmax)
        real*4 gobsm(monmax,nvarmax,nvarmax)

        real*4 cmody(nvarmax,nvarmax)
        real*4 gmody(nvarmax,nvarmax)
        real*4 cobsy(nvarmax,nvarmax)
        real*4 gobsy(nvarmax,nvarmax)

        real*4 amx(nvarmax),amn(nvarmax)

        real*4 gmg(nvarmax,nvarmax),temp(nvarmax,nvarmax)
        real*4 bt(nvarmax),btprev(nvarmax),gprev(nvarmax)
        real*4 go(nvarmax,nvarmax),co(nvarmax,nvarmax)
        real*4 temp2(nvarmax,nvarmax),temp3(nvarmax)
        real*4 cmg(nvarmax,nvarmax),temp4(nvarmax)
        real*4 temp1(nvarmax),miss
        real*4 phll(5,10,nvarmax),phul(5,10,nvarmax),thres(nvarmax)
        real*4 phlwr(nvarmax),phupr(nvarmax)

        real*4 avdt(nvarmax,monmax,ndmax),sddt(nvarmax,monmax,ndmax)
        real*4 avmt(nvarmax,monmax),sdmt(nvarmax,monmax)
        real*4 avyt(nvarmax),sdyt(nvarmax)


        ww=1.96

        do k=1,5
            do i=1,10
                do j=1,nvarmax
                    phll(k,i,j)=100000.0
                    phul(k,i,j)=-100000.0
                enddo
            enddo
        enddo

        call day(nday,monmax)

c	write(*,*)'      MRNBC'
c	write(*,*)'      Calculating statistics of obs. data'
c	reanalysis data
        inx=0
        if(itime.ne.0) goto 9

c	find daily smoothened mean, sd and corl of reanalysis data
        call sdsmooth(rec,avdh,sddh,cordh,iband,nvar,nday,nycur,ns,
     1                  3,leap,idays,miss,amn,amx,nout,nvarmax,
     2                  nyrmax,monmax,ndmax,nnmax)


c	calculate matrices C and G 
        call C_G_crs_daily(rec,crsod,iband,nvar,nday,nycur,ns,
     1                    3,leap,idays,inx,miss,nout,ilimit,thres,
     2                    nvarmax,nyrmax,monmax,ndmax,nnmax)


        call C_G_corl_daily(rec,cobs,gobs,iband,nvar,nday,nycur,ns,
     1                      3,leap,idays,inx,miss,nout,ilimit,thres,
     2                      nvarmax,nyrmax,monmax,ndmax,nnmax)

9       continue

c	calculate annual, seasonal and monthly series of reanalysis data

        do k=1,nvar
        do i=1,nycur
        sy1=0.0	
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,3)
        if(leap(3).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns,i,jd)
        endif
        endif
        sm1=0.0
        i1=0
        do l=1,jd
        if(rec(k,i,j,l).gt.miss)then
        i1=i1+1
        sm1=sm1+rec(k,i,j,l)
        endif
        enddo
        rem(k,i,j)=sm1/float(i1)
        sy1=sy1+rem(k,i,j)
        enddo
        rey(k,i)=sy1/float(nout)
        enddo
        enddo

c	find monthly means, sds and corl of reanalysis data
        call avsds(rem,nvar,nycur,avmh,sdmh,cormh,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)

c	calculate matrices C and G 
        call C_G_crs_season(rem,nvar,nycur,crsom,inx,
     1      nout,monmax,ilimit,thres,nyrmax,nvarmax)

        call C_G_corl_season(rem,nvar,nycur,cobsm,gobsm,inx,
     1      nout,monmax,ilimit,thres,nyrmax,nvarmax)

c	find annual, sds and corl of reanalysis data
        call avsdy(rey,nvar,nycur,avyh,sdyh,coryh,
     1      amn,amx,nyrmax,nvarmax)

c	calculate matrices C and G 
        call C_G_crs_year(rey,nvar,nycur,crsoy,
     1      inx,ilimit,thres,nyrmax,nvarmax)

        call C_G_corl_year(rey,nvar,nycur,cobsy,gobsy,
     1      inx,ilimit,thres,nyrmax,nvarmax)

c**********************************************
c&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
c	calculate mean of obs climate series and store
        do k=1,nvar
        avr(k)=0.0
        i1=0
        do i=1,nycur
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,3)
        if(leap(3).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns,i,jd)
        endif
        endif
        do l=1,jd
        i1=i1+1
        avh(k)=avh(k)+rec(k,i,j,l)
        enddo
        enddo
        enddo
        avh(k)=avh(k)/float(i1)
        enddo

c	calculate mean of raw current climate series and store
        do k=1,nvar
        avr(k)=0.0
        i1=0
        do i=1,ngcur
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,1)
        if(leap(1).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        endif
        endif
        do l=1,jd
        i1=i1+1
        avr(k)=avr(k)+gcmc(k,i,j,l)
        enddo
        enddo
        enddo
        avr(k)=avr(k)/float(i1)
        enddo
        avr=avh

c&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        call QM_dm(rec,gcmc,gcmf,ns,nsgc,nycur,ngcur,nvar,thres,
     1             ilimit,itime,ngfut,nsgf,leap,idays,nout,phlwr,
     2             phupr,avh,avr,nvarmax,nyrmax,monmax,ndmax,nnmax)

c	write(*,*)'reached here'
        inx=2

c	write(*,*)'    Running bias correction'
c	itr=1 corercts for biases in SDs at higher time scale
c	itr=2 corercts for biases in LAG0 cross correlations
c	itr=3 corercts for biases in LAG1 auto correlations
c	itr=4 corercts for biases in LAG1 cross correlations

        nntr=4	
        do itr=1,nntr
        ii1=0
        ii2=0
        do jj=1,2
        if(jj.eq.1)then
        iyr=ngcur
        ns1=nsgc
        else
        iyr=ngfut
        ns1=nsgf
        endif

c	extract daily series
        do i=1,iyr
        if(jj.gt.1)ii1=ii1+1
        i1=0
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        i1=i1+1
        do k=1,nvar
        if(jj.eq.1)gd(k,i,j,l)=gcmc(k,i,j,l)
        if(jj.gt.1)gd(k,i,j,l)=gcmf(k,i,j,l)
        ggd(k,i,j,l)=gd(k,i,j,l)
        gdct(k,i,j,l)=gd(k,i,j,l)
        enddo
        enddo
        enddo
        enddo

        if(itr.eq.1.or.itime.eq.0)goto 301
        if(itr.eq.2)then
c	correct for bias in cross corl
c	find smoothened mean and sd of gcm mean and sd corrected series
        ggd=gdct
        call sdsmooth(ggd,avd,sdd,cord,iband,nvar,nday,iyr,ns1,
     1                jj,leap,idays,miss,amn,amx,nout,nvarmax,
     2                nyrmax,monmax,ndmax,nnmax)
        if(jj.eq.1) call C_G_crs_daily(ggd,crsmd,iband,nvar,
     1      nday,iyr,ns1,jj,leap,idays,inx,miss,nout,ilimit,thres,
     2      nvarmax,nyrmax,monmax,ndmax,nnmax)
        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        gmg=0.0
        go=0.0

        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=crsmd(j,l,k1,k2)
        go(k1,k2)=crsod(j,l,k1,k2)
        enddo
        enddo

        DO K=1,NVAR
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        bt(k)=(ggd(k,i,j,l)-avd(k,j,l))/fact
        a(k)=ggd(k,i,j,l)
        enddo
        temp=0.0
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,gcur,nvar,nvarmax)
        in=0
        do k=1,nvar
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        gdct(k,i,j,l)=gcur(k)*fact+avd(k,j,l)
        if(gdct(k,i,j,l).gt.phul(1,jj,k))in=1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))in=1
        if(gdct(k,i,j,l).gt.phul(1,jj,k))sm3=sm3+1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))sm3=sm3+0.25
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gdct(k,i,j,l)=a(k)
        enddo
        endif
        enddo
        enddo
        enddo

c	check to see if bias correction in dependence has changed the values significantly
        call sdsmooth(gdct,avdt,sddt,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     2                       nyrmax,monmax,ndmax,nnmax)
        sm1=0.0
        sm2=0.0
        nn=(2*iband+1)*iyr
        an=float(nn)
        do j=1,nout
        do l=1,nday(j)
        do k=1,nvar
        if(abs((avdt(k,j,l)-avd(k,j,l))).gt.
     1      sdd(k,j,l)*ww/sqrt(an))sm1=sm1+1
        if(abs((sddt(k,j,l)-sdd(k,j,l))).gt.
     1       (ww/sqrt(2*(an-1)))*sdd(k,j,l))sm1=sm1+1
        sm2=sm2+2
        enddo
c	pause
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gdct=ggd
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in daily L0 cross dependence'

        goto 301
        endif
        if(itr.eq.3)then
c	correct for bias in auto corl
c	find smoothened mean and sd of gcm mean and sd corrected series
        ggd=gdct
        if(jj.eq.1)call sdsmooth(ggd,avd,sdd,cordc,iband,nvar,nday,iyr,
     1              ns1,jj,leap,idays,miss,amn,amx,nout,nvarmax,
     4                       nyrmax,monmax,ndmax,nnmax)
        call sdsmooth(ggd,avd,sdd,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     4                       nyrmax,monmax,ndmax,nnmax)
        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd

        DO K=1,NVAR
c	write(*,*)k,j,l,sdd(k,j,l)
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        bt(k)=(ggd(k,i,j,l)-avd(k,j,l))/fact
        if(i.eq.1.and.j.eq.1.and.l.eq.1.and.i.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggd(k,i,j,l)
        enddo
        in=0
        do k=1,nvar
        fact=sqrt((1.0-cordh(k,j,l)*cordh(k,j,l))/(1.0-cordc(k,j,l)
     1          *cordc(k,j,l)))
        gcur(k)=cordh(k,j,l)*gprev(k)+fact
     1          *(bt(k)-cordc(k,j,l)*btprev(k))
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        gdct(k,i,j,l)=gcur(k)*fact+avd(k,j,l)
        if(gdct(k,i,j,l).gt.phul(1,jj,k))in=1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))in=1
        if(gdct(k,i,j,l).gt.phul(1,jj,k))sm3=sm3+1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))sm3=sm3+0.25
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gdct(k,i,j,l)=a(k)
        enddo
        endif
        enddo
        enddo
        enddo
c	check to see if bias correction in dependence has changed the values significantly
        call sdsmooth(gdct,avdt,sddt,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     4                       nyrmax,monmax,ndmax,nnmax)
        sm1=0.0
        sm2=0.0
        nn=(2*iband+1)*iyr
        an=float(nn)
        do j=1,nout
        do l=1,nday(j)
        do k=1,nvar
        if(abs((avdt(k,j,l)-avd(k,j,l))).gt.
     1      sdd(k,j,l)*ww/sqrt(an))sm1=sm1+1
        if(abs((sddt(k,j,l)-sdd(k,j,l))).gt.
     1       (ww/sqrt(2*(an-1)))*sdd(k,j,l))sm1=sm1+1
        sm2=sm2+2
        enddo
c	pause
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gdct=ggd
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in daily L1 Auto dependence'
        goto 301
        endif
        if(itr.eq.4)then
c	correct for bias in cross corl
c	find smoothened mean and sd of gcm mean and sd corrected series
        ggd=gdct
        call sdsmooth(ggd,avd,sdd,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     4                       nyrmax,monmax,ndmax,nnmax)
        if(jj.eq.1)call C_G_corl_daily(ggd,cmod,gmod,iband,nvar,
     1           nday,iyr,ns1,jj,leap,idays,inx,miss,nout,ilimit,thres,
     2           nvarmax,nyrmax,monmax,ndmax,nnmax)
        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        gmg=0.0
        cmg=0.0
        go=0.0
        co=0.0

        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=gmod(j,l,k1,k2)
        cmg(k1,k2)=cmod(j,l,k1,k2)

        go(k1,k2)=gobs(j,l,k1,k2)
        co(k1,k2)=cobs(j,l,k1,k2)
        enddo
        enddo

        DO K=1,NVAR
c	write(*,*)k,j,l,sdd(k,j,l)
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        bt(k)=(ggd(k,i,j,l)-avd(k,j,l))/fact
        if(i.eq.1.and.j.eq.1.and.l.eq.1.and.i.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggd(k,i,j,l)
        enddo
        temp=0.0
        temp1=0.0
        temp2=0.0
        temp3=0.0
        temp4=0.0
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,temp1,nvar,nvarmax)

        call matmul(co,gprev,temp4,nvar,nvarmax)

        call matmat(temp,cmg,temp2,nvar,nvarmax)
        call matmul(temp2,btprev,temp3,nvar,nvarmax)
        in=0
        do k=1,nvar
        gcur(k)=temp4(k)+temp1(k)-temp3(k)
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdd(k,j,l)
        if(sdd(k,j,l).lt.0.01)fact=1.0
        gdct(k,i,j,l)=gcur(k)*fact+avd(k,j,l)
        if(gdct(k,i,j,l).gt.phul(1,jj,k))in=1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))in=1
        if(gdct(k,i,j,l).gt.phul(1,jj,k))sm3=sm3+1
        if(gdct(k,i,j,l).lt.phll(1,jj,k))sm3=sm3+0.25
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gdct(k,i,j,l)=a(k)
        enddo
        endif
        enddo
        enddo
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
c	check to see if bias correction in dependence has changed the values significantly
        call sdsmooth(gdct,avdt,sddt,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     4                       nyrmax,monmax,ndmax,nnmax)
        sm1=0.0
        sm2=0.0
        nn=(2*iband+1)*iyr
        an=float(nn)
        do j=1,nout
        do l=1,nday(j)
        do k=1,nvar
        if(abs((avdt(k,j,l)-avd(k,j,l))).gt.
     1      sdd(k,j,l)*ww/sqrt(an))sm1=sm1+1
        if(abs((sddt(k,j,l)-sdd(k,j,l))).gt.
     1       (ww/sqrt(2*(an-1)))*sdd(k,j,l))sm1=sm1+1
        sm2=sm2+2
        enddo
c	pause
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gdct=ggd
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in daily L1 Cross dependence'
        endif


301     continue

c	form monthly series of corrected daily gcm series and store
        do k=1,nvar
        do i=1,iyr
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        sm1=0.0
        do l=1,jd
        sm1=sm1+gdct(k,i,j,l)
        enddo
        gm(k,i,j)=sm1/float(jd)
        ggm(k,i,j)=gm(k,i,j)
        gmct(k,i,j)=gm(k,i,j)
        enddo
        enddo
        enddo
        if(itr.eq.1)then
c	look at the bias in monthly mean and sd
c	find mean and sd of gcm monthly series
        if(jj.eq.1)call avsds(gm,nvar,iyr,avmc,sdm,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)

c	correct for bias in monthly mean
        do k=1,nvar
        do i=1,iyr
        do j=1,nout
        ggm(k,i,j)=(gm(k,i,j)-avmc(k,j))+avmh(k,j)
        if(itime.ne.0)then
        if(ggm(k,i,j).gt.phupr(k))ggm(k,i,j)=gm(k,i,j)
        if(ggm(k,i,j).lt.phlwr(k))ggm(k,i,j)=gm(k,i,j)
        endif
        gmct(k,i,j)=ggm(k,i,j)
        enddo
        enddo
        enddo
c	find mean and sd of gcm mean corrected monthly series
        if(jj.eq.1)call avsds(ggm,nvar,iyr,avm,sdmc,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)
        call avsds(ggm,nvar,iyr,avm,sdm,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)
c	correct for bias in monthly sd
        do k=1,nvar
        do i=1,iyr
        do j=1,nout
        if(sdmh(k,j).lt.0.1.and.sdmc(k,j).lt.0.1)then
        fact=1.0
        else	
        fact=sdmh(k,j)/sdmc(k,j)
        endif
        aa=ggm(k,i,j)
        ggm(k,i,j)=(ggm(k,i,j)-avm(k,j))*fact+avm(k,j)
        if(itime.ne.0)then
        if(ggm(k,i,j).gt.phupr(k))ggm(k,i,j)=aa
        if(ggm(k,i,j).lt.phlwr(k))ggm(k,i,j)=aa
        endif
        gmct(k,i,j)=ggm(k,i,j)
        enddo
        enddo
        enddo
        goto 302
        endif
        if(itr.eq.2)then
c	correct for bias in Lag0 cross corl
c	find mean and sd of gcm mean and sd corrected monthly series
        ggm=gmct

        call avsds(ggm,nvar,iyr,avm,sdm,corm,nout,amn,amx,monmax,
     1           nvarmax,nyrmax)
        if(jj.eq.1) call C_G_crs_season(ggm,nvar,iyr,crsmm,
     1      inx,nout,monmax,ilimit,thres,nyrmax,nvarmax)

        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout
        gmg=0.0
        go=0.0
        temp=0.0

        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=crsmm(j,k1,k2)
        go(k1,k2)=crsom(j,k1,k2)
        enddo
        enddo

        DO K=1,NVAR
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        bt(k)=(ggm(k,i,j)-avm(k,j))/fact
        a(k)=ggm(k,i,j)
        enddo
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,gcur,nvar,nvarmax)
        in=0
        do k=1,nvar
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        gmct(k,i,j)=gcur(k)*fact+avm(k,j)
        if(gmct(k,i,j).gt.phul(2,jj,k))in=1
        if(gmct(k,i,j).lt.phll(2,jj,k))in=1
        if(gmct(k,i,j).gt.phul(2,jj,k))sm3=sm3+1
        if(gmct(k,i,j).lt.phll(2,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gmct(k,i,j)=a(k)
        enddo
        endif
        enddo
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsds(gmct,nvar,iyr,avmt,sdmt,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)

        sm1=0.0
        sm2=0.0
        nn=iyr
        an=float(nn)
        do j=1,nout
        do k=1,nvar
        if(abs(avmt(k,j)-avm(k,j)).gt.sdm(k,j)*ww/sqrt(an))sm1=sm1+1
        if(abs(sdmt(k,j)-sdm(k,j)).gt.(ww/sqrt(2*(an-1)))*sdm(k,j))
     1      sm1=sm1+1
        sm2=sm2+2
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gmct=ggm
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in monthly L0 cross dependence'
        goto 302
        endif
        if(itr.eq.3)then
c	correct for bias in auto corl
c	find mean and sd of gcm mean and sd corrected monthly series
        ggm=gmct
        if(jj.eq.1)call avsds(ggm,nvar,iyr,avm,sdm,cormc,nout,
     1      amn,amx,monmax,nvarmax,nyrmax)
        call avsds(ggm,nvar,iyr,avm,sdm,corm,nout,amn,amx,monmax,
     1      nvarmax,nyrmax)

        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout

        DO K=1,NVAR
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        bt(k)=(ggm(k,i,j)-avm(k,j))/fact
        if(i.eq.1.and.j.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggm(k,i,j)
        enddo
        in=0
        do k=1,nvar
        fact=sqrt((1.0-cormh(k,j)*cormh(k,j))/(1.0-cormc(k,j)
     1           *cormc(k,j)))
        gcur(k)=cormh(k,j)*gprev(k)+fact*(bt(k)-cormc(k,j)
     1          *btprev(k))
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        gmct(k,i,j)=gcur(k)*fact+avm(k,j)
        if(gmct(k,i,j).gt.phul(2,jj,k))in=1
        if(gmct(k,i,j).lt.phll(2,jj,k))in=1
        if(gmct(k,i,j).gt.phul(2,jj,k))sm3=sm3+1
        if(gmct(k,i,j).lt.phll(2,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gmct(k,i,j)=a(k)
        enddo
        endif
        enddo
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsds(gmct,nvar,iyr,avmt,sdmt,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)

        sm1=0.0
        sm2=0.0
        nn=iyr
        an=float(nn)
        do j=1,nout
        do k=1,nvar
        if(abs(avmt(k,j)-avm(k,j)).gt.sdm(k,j)*ww/sqrt(an))sm1=sm1+1
        if(abs(sdmt(k,j)-sdm(k,j)).gt.(ww/sqrt(2*(an-1)))*sdm(k,j))
     1      sm1=sm1+1
        sm2=sm2+2
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gmct=ggm
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in monthly L1 Auto dependence'
        goto 302
        endif
        if(itr.eq.4)then
c	correct for bias in cross corl
c	find mean and sd of gcm mean and sd corrected monthly series
        ggm=gmct
        call avsds(ggm,nvar,iyr,avm,sdm,corm,nout,amn,amx,monmax,
     1           nvarmax,nyrmax)
        if(jj.eq.1)call C_G_corl_season(ggm,nvar,iyr,cmodm,gmodm,
     1      inx,nout,monmax,ilimit,thres,nyrmax,nvarmax)

        sm3=0.0
        sm4=0.0
        do i=1,iyr
        do j=1,nout
        gmg=0.0
        cmg=0.0
        go=0.0
        co=0.0
        temp=0.0
        temp1=0.0
        temp2=0.0
        temp3=0.0
        temp4=0.0

        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=gmodm(j,k1,k2)
        cmg(k1,k2)=cmodm(j,k1,k2)

        go(k1,k2)=gobsm(j,k1,k2)
        co(k1,k2)=cobsm(j,k1,k2)
        enddo
        enddo

        DO K=1,NVAR
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        bt(k)=(ggm(k,i,j)-avm(k,j))/fact
        if(i.eq.1.and.j.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggm(k,i,j)
        enddo
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,temp1,nvar,nvarmax)

        call matmul(co,gprev,temp4,nvar,nvarmax)

        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmat(temp,cmg,temp2,nvar,nvarmax)
        call matmul(temp2,btprev,temp3,nvar,nvarmax)
        in=0
        do k=1,nvar
        gcur(k)=temp4(k)+temp1(k)-temp3(k)
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdm(k,j)
        if(sdm(k,j).lt.0.01)fact=1.0
        gmct(k,i,j)=gcur(k)*fact+avm(k,j)
        if(gmct(k,i,j).gt.phul(2,jj,k))in=1
        if(gmct(k,i,j).lt.phll(2,jj,k))in=1
        if(gmct(k,i,j).gt.phul(2,jj,k))sm3=sm3+1
        if(gmct(k,i,j).lt.phll(2,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gmct(k,i,j)=a(k)
        enddo
        endif
        enddo
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsds(gmct,nvar,iyr,avmt,sdmt,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)

        sm1=0.0
        sm2=0.0
        nn=iyr
        an=float(nn)
        do j=1,nout
        do k=1,nvar
        if(abs(avmt(k,j)-avm(k,j)).gt.sdm(k,j)*ww/sqrt(an))sm1=sm1+1
        if(abs(sdmt(k,j)-sdm(k,j)).gt.(ww/sqrt(2*(an-1)))*sdm(k,j))
     1      sm1=sm1+1
        sm2=sm2+2
        enddo
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gmct=ggm
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in monthly L1 cross dependence'
        endif

302     continue

c	form annual series of corrected seasonal gcm series and store
        do k=1,nvar
        do i=1,iyr
        sy1=0.0
        do j=1,nout
        sy1=sy1+gmct(k,i,j)
        enddo
        gy(k,i)=sy1/float(nout)
        ggy(k,i)=gy(k,i)
        gyct(k,i)=gy(k,i)
        enddo
        enddo
        if(itr.eq.1)then
c	correct for bias in annual mean
c	find mean and sd of gcm annual series
        if(jj.eq.1)call avsdy(gy,nvar,iyr,avyc,sdy,cory,amn,amx,
     1           nyrmax,nvarmax)
        do k=1,nvar
        do i=1,iyr
        ggy(k,i)=(gy(k,i)-avyc(k))+avyh(k)
        if(ggy(k,i).gt.phupr(k))ggy(k,i)=gy(k,i)
        if(ggy(k,i).lt.phlwr(k))ggy(k,i)=gy(k,i)
        gyct(k,i)=ggy(k,i)
        enddo
        enddo
c	find mean and sd of gcm annual mean corrected series
        if(jj.eq.1)call avsdy(ggy,nvar,iyr,avy,sdyc,cory,
     1      amn,amx,nyrmax,nvarmax)
        call avsdy(ggy,nvar,iyr,avy,sdy,cory,amn,amx,nyrmax,nvarmax)

c	correct for bias in annual sd
        do k=1,nvar
        do i=1,iyr
        if(sdyh(k).lt.0.1.and.sdyc(k).lt.0.1)then
        fact=1.0
        else	
        fact=sdyh(k)/sdyc(k)
        endif
        aa=ggy(k,i)
        ggy(k,i)=(ggy(k,i)-avy(k))*fact+avy(k)
        if(ggy(k,i).gt.phupr(k))ggy(k,i)=aa
        if(ggy(k,i).lt.phlwr(k))ggy(k,i)=aa
        gyct(k,i)=ggy(k,i)
        enddo
        enddo
        goto 306
        endif
        if(itr.eq.2)then
c	correct for bias in cross corl
c	find mean and sd of gcm annual mean and sd corrected series
        ggy=gyct
        call avsdy(ggy,nvar,iyr,avy,sdy,cory,amn,amx,nyrmax,nvarmax)
        if(jj.eq.1)call C_G_crs_year(ggy,nvar,iyr,crsmy,inx,
     1      ilimit,thres,nyrmax,nvarmax)

        gmg=0.0
        go=0.0
        temp=0.0
        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=crsmy(k1,k2)
        go(k1,k2)=crsoy(k1,k2)
        enddo
        enddo

        sm3=0.0
        sm4=0.0
        do i=1,iyr
        DO K=1,NVAR
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        bt(k)=(ggy(k,i)-avy(k))/fact
        a(k)=ggy(k,i)
        enddo
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,gcur,nvar,nvarmax)

        in=0
        do k=1,nvar
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        gyct(k,i)=gcur(k)*fact+avy(k)
        if(gyct(k,i).gt.phul(3,jj,k))in=1
        if(gyct(k,i).lt.phll(3,jj,k))in=1
        if(gyct(k,i).gt.phul(3,jj,k))sm3=sm3+1
        if(gyct(k,i).lt.phll(3,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gyct(k,i)=a(k)
        enddo
        endif
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsdy(gyct,nvar,iyr,avyt,sdyt,cory,amn,amx,nyrmax,nvarmax)
        an=float(iyr)
        sm1=0.0
        sm2=0.0
        do k=1,nvar
        if(abs((avyt(k)-avy(k))).gt.sdy(k)*ww/sqrt(an))sm1=sm1+1
        if(abs((sdyt(k)-sdy(k))).gt.
     1       (ww/sqrt(2*(an-1)))*sdy(k))sm1=sm1+1
        sm2=sm2+2
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gyct=ggy
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in annual L0 cross dependence'
        goto 306
        endif
        if(itr.eq.3)then
c	correct for bias in auto corl
c	find mean and sd of gcm annual mean and sd corrected series
        ggy=gyct
        if(jj.eq.1)call avsdy(ggy,nvar,iyr,avy,sdy,coryc,amn,amx,
     1           nyrmax,nvarmax)
        call avsdy(ggy,nvar,iyr,avy,sdy,cory,amn,amx,nyrmax,nvarmax)
        sm3=0.0
        sm4=0.0
        do i=1,iyr
        DO K=1,NVAR
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        bt(k)=(ggy(k,i)-avy(k))/fact
        if(i.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggy(k,i)
        enddo

        in=0
        do k=1,nvar
        fact=sqrt((1.0-coryh(k)*coryh(k))/(1.0-coryc(k)*coryc(k)))
        gcur(k)=coryh(k)*gprev(k)+fact*(bt(k)-coryc(k)*btprev(k))
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        gyct(k,i)=gcur(k)*fact+avy(k)
        if(gyct(k,i).gt.phul(3,jj,k))in=1
        if(gyct(k,i).lt.phll(3,jj,k))in=1
        if(gyct(k,i).gt.phul(3,jj,k))sm3=sm3+1
        if(gyct(k,i).lt.phll(3,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gyct(k,i)=a(k)
        enddo
        endif
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsdy(gyct,nvar,iyr,avyt,sdyt,cory,amn,amx,nyrmax,nvarmax)
        an=float(iyr)
        sm1=0.0
        sm2=0.0
        do k=1,nvar
        if(abs((avyt(k)-avy(k))).gt.sdy(k)*ww/sqrt(an))sm1=sm1+1
        if(abs((sdyt(k)-sdy(k))).gt.
     1       (ww/sqrt(2*(an-1)))*sdy(k))sm1=sm1+1
        sm2=sm2+2
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gyct=ggy
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in annual L1 Auto dependence'
        goto 306
        endif
        if(itr.eq.4)then
c	correct for bias in cross corl
c	find mean and sd of gcm annual mean and sd corrected series
        ggy=gyct
        call avsdy(ggy,nvar,iyr,avy,sdy,cory,amn,amx,nyrmax,nvarmax)
        if(jj.eq.1)call C_G_corl_year(ggy,nvar,iyr,cmody,gmody,inx,
     1      ilimit,thres,nyrmax,nvarmax)

        gmg=0.0
        cmg=0.0
        go=0.0
        co=0.0
        temp=0.0
        temp1=0.0
        temp2=0.0
        temp3=0.0
        temp4=0.0

        do k1=1,nvar
        do k2=1,nvar 
        gmg(k1,k2)=gmody(k1,k2)
        cmg(k1,k2)=cmody(k1,k2)

        go(k1,k2)=gobsy(k1,k2)
        co(k1,k2)=cobsy(k1,k2)
        enddo
        enddo

        sm3=0.0
        sm4=0.0
        do i=1,iyr
        DO K=1,NVAR
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        bt(k)=(ggy(k,i)-avy(k))/fact
        if(i.eq.1)then
        gprev(k)=bt(k)
        btprev(k)=bt(k)
        endif
        a(k)=ggy(k,i)
        enddo
        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmul(temp,bt,temp1,nvar,nvarmax)

        call matmul(co,gprev,temp4,nvar,nvarmax)

        call matmat(go,gmg,temp,nvar,nvarmax)
        call matmat(temp,cmg,temp2,nvar,nvarmax)
        call matmul(temp2,btprev,temp3,nvar,nvarmax)

        in=0
        do k=1,nvar
        gcur(k)=temp4(k)+temp1(k)-temp3(k)
        gprev(k)=gcur(k)
        btprev(k)=bt(k)
        fact=sdy(k)
        if(sdy(k).lt.0.01)fact=1.0
        gyct(k,i)=gcur(k)*fact+avy(k)
        if(gyct(k,i).gt.phul(3,jj,k))in=1
        if(gyct(k,i).lt.phll(3,jj,k))in=1
        if(gyct(k,i).gt.phul(3,jj,k))sm3=sm3+1
        if(gyct(k,i).lt.phll(3,jj,k))sm3=sm3+1
        sm4=sm4+2
        enddo
        if(in.gt.0)then
        do k=1,nvar
        gyct(k,i)=a(k)
        enddo
        endif
        enddo
c	check for averages to see if bias correction in dependence has changed the values significantly
        call avsdy(gyct,nvar,iyr,avyt,sdyt,cory,amn,amx,nyrmax,nvarmax)
        an=float(iyr)
        sm1=0.0
        sm2=0.0
        do k=1,nvar
        if(abs((avyt(k)-avy(k))).gt.sdy(k)*ww/sqrt(an))sm1=sm1+1
        if(abs((sdyt(k)-sdy(k))).gt.
     1       (ww/sqrt(2*(an-1)))*sdy(k))sm1=sm1+1
        sm2=sm2+2
        enddo
        if(sm1.gt.sm2*0.001.or.sm3.gt.sm4*0.001)gyct=ggy
c	if(sm1.le.sm2*0.001.and.sm3.le.sm4*0.001)write(*,*)' Itr ',
c	1itr,jj,' Considering corr in annual L1 cross dependence'
        endif

306     continue

        if(itr.eq.1)then
c	write(*,*)'      Defining lower and upper bounds on the data'
c	apply final correction
        do i=1,iyr
        i1=0
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        sm1=0.0
        do l=1,jd
        i1=i1+1
        do k=1,nvar
        if(jj.eq.1)aa=gcmc(k,i,j,l)
        if(jj.gt.1)aa=gcmf(k,i,j,l)

        if(aa.gt.miss)then
        af1=1.0
        af2=1.0
        af3=1.0
        if(itime.eq.0)then
        if(gd(k,i,j,l).ne.0.0)af1=gdct(k,i,j,l)/gd(k,i,j,l)
        endif
        ac1=aa*af1
        if(ac1.gt.phupr(k))af1=1.0
        if(ac1.lt.phlwr(k))af1=1.0
        if(gm(k,i,j).ne.0.0)af2=gmct(k,i,j)/gm(k,i,j)
        ac1=aa*af2
        if(ac1.gt.phupr(k))af2=1.0
        if(ac1.lt.phlwr(k))af2=1.0
        if(gy(k,i).ne.0.0)af3=gyct(k,i)/gy(k,i)
        ac1=aa*af3
        if(ac1.gt.phupr(k))af3=1.0
        if(ac1.lt.phlwr(k))af3=1.0
        ac=af1*af2*af3
        if(ilimit(k).gt.0)then
        if(aa.lt.thres(k).and.aa.ge.0.0)ac=1.0
        if(aa.ge.thres(k).and.aa*ac.lt.thres(k))
     1      ac=1.001*(thres(k)/aa)
        endif
        ac1=aa*ac
        if(ac1.gt.phupr(k))ac1=aa
        if(ac1.lt.phlwr(k))ac1=aa
        if(jj.eq.1)gcm(k,i,j,l)=ac1
        if(jj.gt.1)gcmf(k,i,j,l)=ac1
        else
        if(jj.eq.1)gcm(k,i,j,l)=miss
        if(jj.gt.1)gcmf(k,i,j,l)=miss
        endif

        enddo

        enddo
        enddo
        enddo

        do i=1,iyr
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        do k=1,nvar
        if(jj.eq.1)gdct(k,i,j,l)=gcm(k,i,j,l)
        if(jj.gt.1)gdct(k,i,j,l)=gcmf(k,i,j,l)
        enddo
        enddo
        enddo
        enddo

        call sdsmooth(gdct,avdt,sddt,cord,iband,nvar,nday,iyr,ns1,
     1              jj,leap,idays,miss,amn,amx,nout,nvarmax,
     2                       nyrmax,monmax,ndmax,nnmax)

        do k=1,nvar
        do i=1,iyr
        i1=0
        do j=1,nout
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        sm1=0.0
        sm2=0.0
        do l=1,jd
        i1=i1+1
        if(gdct(k,i,j,l).gt.miss)then
        sm1=sm1+gdct(k,i,j,l)
        sm2=sm2+1
        ac1=gdct(k,i,j,l)
        if(ac1+sddt(k,j,l).gt.phul(1,jj,k))
     1   phul(1,jj,k)=ac1+sddt(k,j,l)
        if(ac1-sddt(k,j,l).lt.phll(1,jj,k))
     1   phll(1,jj,k)=ac1-sddt(k,j,l)
        endif
        enddo
        gmct(k,i,j)=0.0
        if(sm2.gt.0)gmct(k,i,j)=sm1/sm2
        enddo
        enddo
        enddo
        call avsds(gmct,nvar,iyr,avmt,sdmt,corm,
     1      nout,amn,amx,monmax,nvarmax,nyrmax)
c	define monthly upper and lower limits
        do k=1,nvar
        do i=1,iyr
        sm1=0.0
        do j=1,nout
        sm1=sm1+gmct(k,i,j)
        ac1=gmct(k,i,j)
        if(ac1+sdmt(k,j).gt.phul(2,jj,k))
     1      phul(2,jj,k)=ac1+sdmt(k,j)
        if(ac1-sdmt(k,j).lt.phll(2,jj,k))
     1      phll(2,jj,k)=ac1-sdmt(k,j)
        enddo
        gyct(k,i)=sm1/float(nout)
        enddo
        enddo
        call avsdy(gyct,nvar,iyr,avyt,sdyt,cory,amn,amx,nyrmax,nvarmax)
c	define annual upper and lower limits
        do k=1,nvar
        do i=1,iyr
        ac1=gyct(k,i)
        if(ac1+sdyt(k).gt.phul(3,jj,k))
     1   phul(3,jj,k)=ac1+sdyt(k)
        if(ac1-sdyt(k).lt.phll(3,jj,k))
     1   phll(3,jj,k)=ac1-sdyt(k)
            enddo
        enddo

c	compare with supplied physical upper and lower limits
        do k=1,nvar
        do j=1,3
        if(phll(j,jj,k).lt.phlwr(k))phll(j,jj,k)=phlwr(k)
        if(phul(j,jj,k).gt.phupr(k))phul(j,jj,k)=phupr(k)
        enddo
        enddo
        goto 618
        endif

c	apply final correction and store final corrected series
        do i=1,iyr
        if(jj.gt.1)ii2=ii2+1
        i1=0
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        sm1=0.0
        do l=1,jd
        i1=i1+1
        do k=1,nvar
        if(jj.eq.1)aa=gcmc(k,i,j,l)
        if(jj.gt.1)aa=gcmf(k,i,j,l)

        if(aa.gt.miss)then
        af1=1.0
        af2=1.0
        af3=1.0
        if(itime.eq.0)then
        if(gd(k,i,j,l).ne.0.0)af1=gdct(k,i,j,l)/gd(k,i,j,l)
        endif
        ac1=aa*af1
        if(ac1.gt.phul(1,jj,k))af1=1.0
        if(ac1.lt.phll(1,jj,k))af1=1.0
        if(gm(k,i,j).ne.0.0)af2=gmct(k,i,j)/gm(k,i,j)
        ac1=aa*af2
        if(ac1.gt.phul(1,jj,k))af2=1.0
        if(ac1.lt.phll(1,jj,k))af2=1.0
        if(gy(k,i).ne.0.0)af3=gyct(k,i)/gy(k,i)
        ac1=aa*af3
        if(ac1.gt.phul(1,jj,k))af3=1.0
        if(ac1.lt.phll(1,jj,k))af3=1.0
        ac=af1*af2*af3
        if(ilimit(k).gt.0)then
        if(aa.lt.thres(k).and.aa.ge.0.0)ac=1.0
        if(aa.ge.thres(k).and.aa*ac.lt.thres(k))
     1   ac=1.001*(thres(k)/aa)
        endif
        ac1=aa*ac
        if(ac1.gt.phul(1,jj,k))ac1=aa
        if(ac1.lt.phll(1,jj,k))ac1=aa

        if(jj.eq.1)gcm(k,i,j,l)=ac1
        if(jj.gt.1)gcmf(k,i,j,l)=ac1
        else
        if(jj.eq.1)gcm(k,i,j,l)=miss
        if(jj.gt.1)gcmf(k,i,j,l)=miss
        endif

        enddo

        enddo
        enddo
        enddo
c	transfer current climate bias corrected value to the raw series
618     continue
        if(jj.gt.1)then
        do i=1,ngcur
        do j=1,nout
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        endif
        endif
        do l=1,jd
        do k=1,nvar
        gcmc(k,i,j,l)=gcm(k,i,j,l)
        enddo
        enddo
        enddo
        enddo
        endif

 619    continue
c	following is the end of do loop for segments
        enddo

c	write(*,*)' Iteration ',itr,' is over'
c	following is the end of do loop for itr
        enddo
c	finally match the mass
c**********************************************
        do jj=1,2
        if(jj.eq.1)then
        iyr=ngcur
        ns1=nsgc
        else
        iyr=ngfut
        ns1=nsgf
        endif
        if(jj.eq.1)call sdsmooth(gcmc,avd,sdd,cord,iband,nvar,
     1           nday,iyr,ns1,jj,leap,idays,miss,amn,amx,
     2           nout,nvarmax,nyrmax,monmax,ndmax,nnmax)
        if(jj.eq.1)ggd=gcmc
        if(jj.eq.2)ggd=gcmf
        do k=1,nvar
        do j=1,nout
        sum1=0.0
        sum2=0.0
        do i=1,iyr
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        ab=ggd(k,i,j,l)-avd(k,j,l)+avdh(k,j,l)
        if(ab.lt.0.99*amx(k))sum1=sum1+ab
        if(ilimit(k).gt.0)then
        if(ggd(k,i,j,l).lt.thres(k).and.ab.ge.thres(k))
     1                 ab=ggd(k,i,j,l)
        if(ggd(k,i,j,l).ge.thres(k).and.ab.lt.thres(k))
     1                 ab=thres(k)
        endif
        ggd(k,i,j,l)=ab
        if(ggd(k,i,j,l).gt.phul(1,jj,k))ggd(k,i,j,l)=phul(1,jj,k)
        if(ggd(k,i,j,l).lt.phll(1,jj,k))ggd(k,i,j,l)=phll(1,jj,k)
        if(ggd(k,i,j,l).lt.0.99*amx(k))sum2=sum2+ggd(k,i,j,l)
        enddo
        enddo
c	apply correction
        do i=1,iyr
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,jj)
        if(leap(jj).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns1,i,jd)
        endif
        endif
        do l=1,jd
        ab=ggd(k,i,j,l)
        if(sum2.ne.0.0.and.ab.lt.0.99*amx(k))ab=ggd(k,i,j,l)*sum1/sum2
        if(ilimit(k).gt.0)then
        if(ggd(k,i,j,l).lt.thres(k).and.ab.ge.thres(k))
     1                 ab=ggd(k,i,j,l)
        if(ggd(k,i,j,l).ge.thres(k).and.ab.lt.thres(k))
     1                 ab=thres(k)
        endif
        ggd(k,i,j,l)=ab
        if(ggd(k,i,j,l).gt.phul(1,jj,k))ggd(k,i,j,l)=phul(1,jj,k)
        if(ggd(k,i,j,l).lt.phll(1,jj,k))ggd(k,i,j,l)=phll(1,jj,k)
        enddo
        enddo
        enddo
        enddo
        if(jj.eq.1)gcmc=ggd
        if(jj.eq.2)gcmf=ggd
        enddo

100     format(a)
298     format(i5,10x,25g10.3)
299     format(2i5,5x,25g10.3)
300     format(3i5,25g10.2)
        return
        end
c***********************************************************
      SUBROUTINE qm_bias(xx,yy,zz,N,NNMAX,avh,avr,acr,acf)
        real*4 xx(nnmax),yy(nnmax),zz(nnmax)
        real*4 a(nnmax),b(nnmax),c(nnmax)
        integer ix(nnmax),iy(nnmax),iz(nnmax)
c	xx=a=observed
c	yy=b=GCM cur
c	zz=c=GCM future

        i5=int(float(n)*0.05+0.5)
        if(i5.lt.2)i5=2
        if(i5.gt.n)i5=n
        do i=1,n
        if(xx(i).lt.-998.0)write(*,*)'Some data is missing'
        if(yy(i).lt.-998.0)write(*,*)'Some data is missing'
        if(zz(i).lt.-998.0)write(*,*)'Some data is missing'
        if(xx(i).lt.-998.0)stop
        if(yy(i).lt.-998.0)stop
        if(zz(i).lt.-998.0)stop
        a(i)=xx(i)
        b(i)=yy(i)
        c(i)=zz(i)
        ix(i)=i
        iy(i)=i
        iz(i)=i
        enddo
        DO 10 I=1,N-1
        K=I+1
        DO 20 J=K,N
        IF(a(j).gt.a(i)) then
        T=a(I)
        a(I)=a(J)
        a(J)=T
        it=ix(i)
        ix(i)=ix(j)
        ix(j)=it
        endif
        IF(b(j).gt.b(i)) then
        T=b(I)
        b(I)=b(J)
        b(J)=T
        it=iy(i)
        iy(i)=iy(j)
        iy(j)=it
        endif
        IF(c(j).gt.c(i)) then
        T=c(I)
        c(I)=c(J)
        c(J)=T
        it=iz(i)
        iz(i)=iz(j)
        iz(j)=it
        endif
20      CONTINUE
10      CONTINUE

        do i=1,n
        c(i)=a(i)+(c(i)-b(i))*avh/avr
        b(i)=a(i)
        enddo

        acr=b(i5)
        acf=c(i5)
c	assign original ranks
        do i=1,n
        zz(iz(i))=c(i)
        yy(iy(i))=b(i)
        enddo
        RETURN
        END
c***************************************************************
      subroutine QM_DM(rec,gcmc,gcmf,ns,nsgc,nycur,ngcur,nvar,thres,
     1           ilimit,itime,ngfut,nsgf,leap,idays,nout,phlwr,phupr,
     2           avh,avr,nvarmax,nyrmax,monmax,ndmax,nnmax)

        real*4 rec(nvarmax,nyrmax,monmax,ndmax),
     1          gcmc(nvarmax,nyrmax,monmax,ndmax),
     2          gcmf(nvarmax,nyrmax,monmax,ndmax)
        REAL*4 avh(nvarmax),avr(nvarmax),thres(nvarmax)
        real*4 xx(nnmax),yy(nnmax),zz(nnmax)
        real*4 simf(nyrmax,ndmax)
        real*4 simc(nyrmax,ndmax)
        real*4 phlwr(nvarmax),phupr(nvarmax)
        integer nday(monmax),leap(4),idays(monmax,4),ilimit(nvarmax)

c	write(*,*)'  QM daily on monthly window'

        call day(nday,monmax)
        do il=1,nvar
        do j=1,nout
        simf=-999.0
        simc=-999.0
        xx=0.0
        yy=0.0
        zz=0.0
c	observed data
        kk=0
        do i=1,nycur
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,3)
        if(leap(3).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(ns,i,jd)
        if(j.eq.2)jd=28
        endif
        endif
        do l=1,jd
        kk=kk+1
        xx(kk)=rec(il,i,j,l)
        enddo
        enddo
        kkh=kk
c	extract data from current climate
        kk=0
        do i=1,ngcur
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,1)
        if(leap(1).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        if(j.eq.2)jd=28
        endif
        endif
        do l=1,jd
        kk=kk+1
        yy(kk)=gcmc(il,i,j,l)
        enddo
        enddo
        kkc=kk
c	extract data for future climate
        kk=0
        do i=1,ngfut
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,2)
        if(leap(2).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,jd)
        if(j.eq.2)jd=28
        endif
        endif
        do l=1,jd
        kk=kk+1
        zz(kk)=gcmf(il,i,j,l)
        enddo
        enddo
        kkf=kk
c	call quantile matching bias correction
        kk=min(kkh,kkc,kkf)
        call qm_bias(xx,yy,zz,kk,nnmax,avh(il),avr(il),acr,acf)
c	substitute bias corrected data for current climate
        kk=0
        do i=1,ngcur
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,1)
        if(leap(1).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        endif
        endif
        do l=1,jd
        if(j.eq.2.and.l.eq.29)then
        simc(i,l)=yy(kk)
        goto 31
        endif
        kk=kk+1
        simc(i,l)=yy(kk)
31      continue
        enddo
        enddo
c	substitute bias corrected data for future climate
        kk=0
        do i=1,ngfut
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,2)
        if(leap(2).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,jd)
        endif
        endif
        do l=1,jd
        if(j.eq.2.and.l.eq.29)then
        simf(i,l)=yy(kk)
        goto 32
        endif
        kk=kk+1
        simf(i,l)=zz(kk)
32      continue
        enddo
        enddo
c$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
c	Check for negatives and keep the mass balance by checking the totals and applying a CORRECTION
c	for current climate
        sim1=0.0
        sim2=0.0
        do i=1,ngcur
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,1)
        if(leap(1).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        endif
        endif
        do l=1,jd
        if(j.eq.2.and.l.eq.29)simc(i,l)=simc(i,l-1)
c	check the values to maintain number of zeros	
        ab=simc(i,l)
        if(ab.lt.acr)sim1=sim1+ab
        if(ilimit(il).gt.0)then
        if(gcmc(il,i,j,l).lt.thres(il).and.ab.ge.thres(il))
     1                 ab=gcmc(il,i,j,l)
        if(gcmc(il,i,j,l).ge.thres(il).and.ab.lt.thres(il))
     1                 ab=thres(il)
        endif
        if(ab.lt.phlwr(il))ab=phlwr(il)
        if(ab.gt.phupr(il))ab=phupr(il)
        if(ab.lt.acr)sim2=sim2+ab
        gcmc(il,i,j,l)=ab
        enddo
        enddo
c	maintain mass balance
        do i=1,ngcur
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,1)
        if(leap(1).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,jd)
        endif
        endif
        do l=1,jd
        if(sim2.ne.0.0.and.gcmc(il,i,j,l).lt.acr)
     1          gcmc(il,i,j,l)=gcmc(il,i,j,l)*sim1/sim2
        if(gcmc(il,i,j,l).lt.phlwr(il))gcmc(il,i,j,l)=phlwr(il)
        if(gcmc(il,i,j,l).gt.phupr(il))gcmc(il,i,j,l)=phupr(il)
        enddo
        enddo

c	for future climate
        sim1=0.0
        sim2=0.0
        do i=1,ngfut
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,2)
        if(leap(2).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,jd)
        endif
        endif
        do l=1,jd
        if(j.eq.2.and.l.eq.29)simf(i,l)=simf(i,l-1)
c	check the values to maintain number of zeros	
        ab=simf(i,l)
        if(ab.lt.acf)sim1=sim1+ab
        if(ilimit(il).gt.0)then
        if(gcmf(il,i,j,l).lt.thres(il).and.ab.ge.thres(il))
     1                 ab=gcmf(il,i,j,l)
        if(gcmf(il,i,j,l).ge.thres(il).and.ab.lt.thres(il))
     1                 ab=thres(il)
        endif
        if(ab.lt.phlwr(il))ab=phlwr(il)
        if(ab.gt.phupr(il))ab=phupr(il)
        if(ab.lt.acf)sim2=sim2+ab
        gcmf(il,i,j,l)=ab
        enddo
        enddo
c	maintain mass balance
        do i=1,ngfut
        if(itime.ne.0)then
        jd=1
        else
        jd=idays(j,2)
        if(leap(2).eq.0)then
        jd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,jd)
        endif
        endif
        do l=1,jd
        if(sim2.ne.0.0.and.gcmf(il,i,j,l).lt.acf)
     1       gcmf(il,i,j,l)=gcmf(il,i,j,l)*sim1/sim2
        if(gcmf(il,i,j,l).lt.phlwr(il))gcmf(il,i,j,l)=phlwr(il)
        if(gcmf(il,i,j,l).gt.phupr(il))gcmf(il,i,j,l)=phupr(il)
        enddo
        enddo
c$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
c	end loop for month
        enddo
c	end loop for variable
        enddo
        return
        end
c***************************************************************
