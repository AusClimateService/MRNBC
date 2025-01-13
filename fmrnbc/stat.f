c       $debug

      subroutine rank(x,n,nnmax)
        real*4 x(nnmax)
        do i=1,n-1
        do j=i+1,n
        if(x(j).lt.x(i))then
        a=x(i)
        x(i)=x(j)
        x(j)=a
        endif
        enddo
        enddo
        return
        end
c*****************************************
      subroutine daycount(ns,i,id)
        integer ns,i,id
            id=28
        if (mod(ns+i,400).eq.0)id=29
            if (mod(ns+i,100).ne.0.and.mod(ns+i,4).eq.0)id=29
            return
            end
c*************************************************
      SUBROUTINE BASIC (data,AVE,SD,N)

c      Implicit double precision (A-H,O-Z) 
        INTEGER n
        REAL ave,var,sd,data(*)
        INTEGER j
        REAL s,ep,an
        an=float(n)
        ave=0.0
        do 11 j=1,n
            ave=ave+data(j)
11       continue
        ave=ave/an
        var=0.0
        ep=0.0
        do 12 j=1,n
            s=data(j)-ave
            ep=ep+s
            var=var+s*s
12           continue
            sd=sqrt((var-ep**2/an)/an)
        return
      END
c******************************************************
      subroutine sdsmooth(atm,av,sd,rho,iband,nvar,nday,ny,ns,ilpg,
     1                   leap,idays,miss,amn,amx,nout,
     2                   nvarmax,nyrmax,monmax,ndmax,nnmax)
c	finds out smoothened mean and sd of GCM series
        real*4 atm(nvarmax,nyrmax,monmax,ndmax)
        real*4 av(nvarmax,monmax,ndmax),sd(nvarmax,monmax,ndmax),
     1       rho(nvarmax,monmax,ndmax),miss,amx(nvarmax),
     1       amn(nvarmax),xx(nnmax),yy(nnmax)  
        integer nday(monmax),idays(monmax,4),leap(4)

        do i=1,nvarmax
        do j=1,monmax
        do k=1,ndmax
        av(i,j,k)=0.0
        sd(i,j,k)=0.0
        rho(i,j,k)=0.0
        enddo
        enddo
        enddo

        do il=1,nvar
        amx(il)=-100000.0
        amn(il)=100000.0
        do i=1,ny
        do j=1,nout
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(ns,i,nd)
        endif
        do l=1,nd
        if(atm(il,i,j,l).ge.miss)then
        if(atm(il,i,j,l).gt.amx(il))amx(il)=atm(il,i,j,l)
        if(atm(il,i,j,l).lt.amn(il))amn(il)=atm(il,i,j,l)
        endif
        enddo
        enddo
        enddo

        do j=1,nout
        nnd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nnd=nday(j)
        do l=1,nnd
        do i=1,nnmax
        xx(i)=0.0
        yy(i)=0.0
        enddo
        kk=0
        do i=1,ny
        if(j.eq.2.and.leap(ilpg).eq.0)then
        call daycount(ns,i,nd)
        if(l.gt.nd)goto 11
        endif
        nw=iband*2+1
        l1=l-iband-1
        do k1=1,nw
        l1=l1+1
        jc=j
        ic=i
        lc=l1
        if(l1.le.0)call day_neg(ic,jc,lc,nout,ns,ilpg,leap,
     1                        idays,0,nday,monmax,indx)
        if(l1.gt.0)call day_pos(ic,jc,lc,nout,ns,ilpg,leap,
     1                        idays,nday,ny,monmax,indx)
        if(indx.eq.1)goto 10
c	check for prev day
        lp=lc-1
        jp=jc
        ip=ic
        if(lp.lt.1)then
        if(ip.eq.1.and.jp.eq.1)goto 10
        jp=jp-1
        if(jp.lt.1)then
        ip=ip-1
        if(ip.lt.1)goto 10
        jp=12
        endif
        lp=idays(jp,ilpg)
        if(leap(ilpg).eq.0)then
        lp=nday(jp)
        if(jp.eq.2)call daycount(ns,ip,lp)
        endif
        endif
        if(atm(il,ic,jc,lc).gt.miss.and.atm(il,ip,jp,lp).gt.miss)then
        kk=kk+1
        xx(kk)=atm(il,ic,jc,lc)
        yy(kk)=atm(il,ip,jp,lp)
        endif
10      continue
        enddo
11      continue
        enddo
        avm=0.0
        avp=0.0
        sdm=0.0
        sdp=0.0
        if(kk.gt.0)call avsdcor(xx,yy,kk,cor,avm,avp,sdm,sdp,nnmax)
        sd(il,j,l)=(sdm+sdp)/2.0
        av(il,j,l)=(avm+avp)/2.0
        rho(il,j,l)=cor
c	if(il.eq.7.and.ny.eq.43)write(16,*)j,l,avm,sdm
        enddo
        enddo
        av(il,2,29)=(av(il,2,28)+av(il,3,1))/2.0
        sd(il,2,29)=(sd(il,2,28)+sd(il,3,1))/2.0
        rho(il,2,29)=(rho(il,2,28)+rho(il,3,1))/2.0
        enddo

        return
        end
c****************************************************************
      subroutine C_G_corl_daily(atm,cobs,dobs,iband,nvar,nday,
     1           ny,ns,ilpg,leap,idays,inx,miss,nout,ilimit,thres,
     2           nvarmax,nyrmax,monmax,ndmax,nnmax)
c	finds out smoothened mean and sd of GCM series
        real*4 atm(nvarmax,nyrmax,monmax,ndmax)
        real*4 cobs(monmax,ndmax,nvarmax,nvarmax)
        real*4 dobs(monmax,ndmax,nvarmax,nvarmax)
        real*4 temp(nvarmax,nvarmax),m1t(nvarmax,nvarmax)
        real*4 m0(nvarmax,nvarmax),m1(nvarmax,nvarmax),thres(nvarmax)
        real*4 co1(nvarmax,nvarmax),go1(nvarmax,nvarmax),miss
        real*4 xx(nnmax),yy(nnmax),zz(nnmax)
        integer nday(monmax),idays(monmax,4),leap(4)
        integer ilimit(nvarmax)

        do j=1,monmax
        do k=1,ndmax
        do i1=1,nvarmax
        do i2=1,nvarmax
        cobs(j,k,i1,i2)=0.0
        dobs(j,k,i1,i2)=0.0
        enddo
        enddo
        enddo
        enddo

        do j=1,nout
        nnd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nnd=nday(j)
        do l=1,nnd

c	write(*,*)j,l

        do i1=1,nvar
        do i2=1,nvar
        do i=1,nnmax
        xx(i)=0.0
        yy(i)=0.0
        zz(i)=0.0
        enddo
        kk=0
        do i=1,ny
        if(leap(ilpg).eq.0.and.j.eq.2)then
        call daycount(ns,i,nd)
        if(l.gt.nd)goto 11
        endif
        nw=iband*2+1
        l1=l-iband-1
        do k1=1,nw
        l1=l1+1
        jc=j
        ic=i
        lc=l1
        if(l1.le.0)call day_neg(ic,jc,lc,nout,ns,ilpg,leap,idays,0,
     1                        nday,monmax,indx)
        if(l1.gt.0)call day_pos(ic,jc,lc,nout,ns,ilpg,leap,idays,
     1                        nday,ny,monmax,indx)
        if(indx.eq.1)goto 10
c	check for prev day
        lp=lc-1
        jp=jc
        ip=ic
        if(lp.lt.1)then
        if(ip.eq.1.and.jp.eq.1)goto 10
        jp=jp-1
        if(jp.lt.1)then
        ip=ip-1
        if(ip.lt.1)goto 10
        jp=12
        endif
        lp=nday(jp)
        if(jp.eq.2)call daycount(ns,ip,lp)
        endif

        if(atm(i1,ic,jc,lc).gt.miss.and.atm(i2,ip,jp,lp).gt.miss
     1    .and.atm(i1,ic,jc,lc).gt.miss.and.atm(i2,ic,jc,lc)
     2    .gt.miss) then
        kk=kk+1
        xx(kk)=atm(i1,ic,jc,lc)
        yy(kk)=atm(i2,ip,jp,lp)
        zz(kk)=atm(i2,ic,jc,lc)
        endif
10      continue
        enddo
11      continue
        enddo
        if(kk.gt.0)call avsdcor(xx,yy,kk,cor,avm,avp,sdm,sdp,nnmax)
        if(cor.ge.1.0)cor=0.9999
        m1(i1,i2)=cor
        if(kk.gt.0)call avsdcor(xx,zz,kk,cor,avm,avp,sdm,sdp,nnmax)
        if(cor.ge.1.0)cor=0.9999
        m0(i1,i2)=cor
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        co1(i1,i2)=0.0
        go1(i1,i2)=0.0
        if(i1.eq.i2)go1(i1,i2)=1.0
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        if(i1.eq.i2)co1(i1,i2)=m1(i1,i2)
        go1(i1,i2)=m0(i1,i2)*(1.0-m1(i1,i1)*m1(i2,i2))
        enddo
        enddo
        call sqroot(go1,nvar,nvarmax)
        do i1=1,nvar
        do i2=1,nvar
        cobs(j,l,i1,i2)=co1(i1,i2)
        dobs(j,l,i1,i2)=go1(i1,i2)
        enddo
        enddo
        
        if(inx.eq.2)then

        temp=0.0
        do i1=1,nvar
        do i2=1,nvar
        temp(i1,i2)=go1(i2,i1)
        enddo
        enddo
        m0=0.0
        call matmat(temp,go1,m0,nvar,nvarmax)
        call solve(m0,nvar,nvarmax)
        go1=0.0
        call matmat(m0,temp,go1,nvar,nvarmax)

        do i1=1,nvar
        do i2=1,nvar
        dobs(j,l,i1,i2)=go1(i1,i2)
        enddo
        enddo
        endif

        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        cobs(2,29,i1,i2)=(cobs(2,28,i1,i2)+cobs(3,1,i1,i2))/2.0
        dobs(2,29,i1,i2)=(dobs(2,28,i1,i2)+dobs(3,1,i1,i2))/2.0
        enddo
        enddo
        return
        end
c****************************************************************
      subroutine C_G_crs_daily(atm,dcrs,iband,nvar,nday,
     1           ny,ns,ilpg,leap,idays,inx,miss,nout,ilimit,thres,
     2           nvarmax,nyrmax,monmax,ndmax,nnmax)
c	finds out smoothened mean and sd of GCM series
        real*4 atm(nvarmax,nyrmax,monmax,ndmax)
        real*4 dcrs(monmax,ndmax,nvarmax,nvarmax)
        real*4 t1(nvarmax,nvarmax),t2(nvarmax,nvarmax),thres(nvarmax)
        real*4 cc(nvarmax,nvarmax),miss
        real*4 xx(nnmax),yy(nnmax)
        integer nday(monmax),idays(monmax,4),leap(4)
        integer ilimit(nvarmax)

        do j=1,monmax
        do k=1,ndmax
        do i1=1,nvarmax
        do i2=1,nvarmax
        dcrs(j,k,i1,i2)=0.0
        enddo
        enddo
        enddo
        enddo

        do j=1,nout
        nnd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nnd=nday(j)
        do l=1,nnd

c	write(*,*)j,l

        do i1=1,nvar
        do i2=1,nvar
        do i=1,nnmax
        xx(i)=0.0
        yy(i)=0.0
        enddo
        kk=0
        do i=1,ny
        if(leap(ilpg).eq.0.and.j.eq.2)then
        call daycount(ns,i,nd)
        if(l.gt.nd)goto 11
        endif
        nw=iband*2+1
        l1=l-iband-1
        do k1=1,nw
        l1=l1+1
        jc=j
        ic=i
        lc=l1
        if(l1.le.0)call day_neg(ic,jc,lc,nout,ns,ilpg,leap,idays,0,
     1                        nday,monmax,indx)
        if(l1.gt.0)call day_pos(ic,jc,lc,nout,ns,ilpg,leap,idays,
     1                        nday,ny,monmax,indx)
        if(indx.eq.1)goto 10

        if(atm(i1,ic,jc,lc).gt.miss.and.atm(i2,ic,jc,lc).gt.miss)then
        kk=kk+1
        xx(kk)=atm(i1,ic,jc,lc)
        yy(kk)=atm(i2,ic,jc,lc)
        endif
10      continue
        enddo
11      continue
        enddo
        if(kk.gt.0)call avsdcor(xx,yy,kk,cor,avm,avp,sdm,sdp,nnmax)
        if(cor.ge.1.0)cor=0.9999
        cc(i1,i2)=cor
        enddo
        enddo
        call sqroot(cc,nvar,nvarmax)
        if(inx.gt.1)then
        temp=0.0
        do i1=1,nvar
        do i2=1,nvar
        t1(i1,i2)=cc(i2,i1)
        enddo
        enddo
        t2=0.0
        call matmat(t1,cc,t2,nvar,nvarmax)
        call solve(t2,nvar,nvarmax)
        cc=0.0
        call matmat(t2,t1,cc,nvar,nvarmax)
        endif

        do i1=1,nvar
        do i2=1,nvar
        dcrs(j,l,i1,i2)=cc(i1,i2)
        enddo
        enddo
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        dcrs(2,29,i1,i2)=(dcrs(2,28,i1,i2)+dcrs(3,1,i1,i2))/2.0
        enddo
        enddo
        return
        end
c****************************************************************
        subroutine day_pos(i,j,l,nout,ns,ilpg,leap,idays,nday,ny,
     1                   monmax,ind)
c	check the day for moving window for positive values of day
        integer nday(monmax),idays(monmax,4),leap(4)
        ind=0
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nd=nday(j)
        if(i.eq.ny.and.j.eq.nout.and.i.eq.ny.and.l.gt.nd)then
        ind=1
        return
        endif
10      nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nd=nday(j)
        if(j.eq.2)call daycount(ns,i,nd)
        if(l.le.nd)then
        return
        endif
        l=l-nd
        j=j+1
        if(j.gt.nout)then
        i=i+1
        j=1
        if(i.gt.ny)then
        ind=1
        return
        endif
        endif
        goto 10
        return
        end
c**********************************************
      subroutine day_neg(i,j,l,nout,ns,ilpg,leap,idays,lag,nday,
     1                   monmax,ind)
c	check the day for moving window for negative values of day
        integer nday(monmax),leap(4),idays(monmax,4)
        ind=0
        if(i.eq.1.and.j.eq.1.and.i.eq.1.and.l-lag.lt.1)then
        ind=1
        return
        endif
10      if(l.gt.0)return
        j=j-1
        if(j.lt.1)then
        j=nout
        i=i-1
        if(i.lt.1)then
        ind=1
        return
        endif
        endif
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)nd=nday(j)
        if(j.eq.2)call daycount(ns,i,nd)
        l=l+nd
        goto 10
        return
        end
c********************************************************************
      subroutine avsds(atm,nvar,ny,av,sd,rho,nout,amn,amx,mnmax,
     1               nvarmax,nyrmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 atm(nvarmax,nyrmax,mnmax),ax(nyrmax,mnmax),
     1              av(nvarmax,mnmax),sd(nvarmax,mnmax)
        real*4 rho(nvarmax,mnmax)
        real*4 amn(nvarmax),amx(nvarmax)
        real*4 xx(nyrmax),yy(nyrmax)
        do i=1,nvarmax
        do j=1,mnmax
        av(i,j)=0.0
        sd(i,j)=0.0
        rho(i,j)=0.0
        enddo
        enddo

        do il=1,nvar
        amx(il)=-10000.0
        amn(il)=10000.0
        do i=1,ny
        do j=1,nout
        if(atm(il,i,j).gt.amx(il))amx(il)=atm(il,i,j)
        if(atm(il,i,j).lt.amn(il))amn(il)=atm(il,i,j)
        enddo
        enddo

        do j=1,nout
        do i=1,nyrmax
        ax(i,j)=0.0
        enddo
        enddo
        do j=1,nout
        do 11 i=1,ny
        ax(i,j)=atm(il,i,j)
11      continue
        enddo
c	for monthly correlations
        do j=1,nout
        ii=0
        do i=1,ny
        if(j.gt.1)then
        ii=ii+1
        xx(ii)=ax(i,j)
        yy(ii)=ax(i,j-1)
        endif
        if(j.eq.1)then
        if(i.gt.1)then
        ii=ii+1
        xx(ii)=ax(i,j)
        yy(ii)=ax(i-1,nout)
        endif
        endif
        enddo
        call avsdcor(xx,yy,ii,cor,avm,avm1,sdm,sdm1,nyrmax)
        rho(il,j)=cor
        enddo
c 	for monthly means and sds
        do j=1,nout
        do i=1,ny
        xx(i)=ax(i,j)
        enddo
        call basic(xx,avm,sdm,ny)
        if(sdm.lt.0.001)sdm=0.001
        sd(il,j)=sdm
        av(il,j)=avm
        enddo

        enddo
        return
        end
c****************************************************************
      subroutine C_g_crs_season(atm,nvar,ny,crs,inx,
     1               nout,mnmax,ilimit,thres,nyrmax,nvarmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 atm(nvarmax,nyrmax,mnmax),crs(mnmax,nvarmax,nvarmax)
        real*4 t1(nvarmax,nvarmax),t2(nvarmax,nvarmax)
        real*4 thres(nvarmax),cc(nvarmax,nvarmax)
        real*4 xx(nyrmax),yy(nyrmax)
        integer ilimit(nvarmax)

        do j=1,mnmax
        do i1=1,nvarmax
        do i2=1,nvarmax
        crs(j,i1,i2)=0.0
        enddo
        enddo
        enddo

        do j=1,nout
c	for corss correlations
        do i1=1,nvar
        do i2=1,nvar
        do i=1,ny
        xx(i)=atm(i1,i,j)
        yy(i)=atm(i2,i,j)
        enddo
        call avsdcor(xx,yy,ny,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        cc(i1,i2)=cor
        enddo
        enddo
        call sqroot(cc,nvar,nvarmax)

        if(inx.eq.2)then
        t1=0.0
        do i1=1,nvar
        do i2=1,nvar
        t1(i1,i2)=cc(i2,i1)
        enddo
        enddo
        t2=0.0
        call matmat(t1,cc,t2,nvar,nvarmax)
        call solve(t2,nvar,nvarmax)
        cc=0.0
        call matmat(t2,t1,cc,nvar,nvarmax)
        endif

        do i1=1,nvar
        do i2=1,nvar
        crs(j,i1,i2)=cc(i1,i2)
        enddo
        enddo

        enddo
        return
        end
c****************************************************************
      subroutine C_g_corl_season(atm,nvar,ny,cobs,dobs,inx,
     1           nout,mnmax,ilimit,thres,nyrmax,nvarmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 atm(nvarmax,nyrmax,mnmax),
     1       cobs(mnmax,nvarmax,nvarmax),dobs(mnmax,nvarmax,nvarmax)
        real*4 temp(nvarmax,nvarmax),m1t(nvarmax,nvarmax)
        real*4 m0(mnmax,nvarmax,nvarmax),m1(mnmax,nvarmax,nvarmax)
        real*4 co1(nvarmax,nvarmax),go1(nvarmax,nvarmax),
     1       m0p(mnmax,nvarmax,nvarmax),thres(nvarmax)
        real*4 xx(nyrmax),yy(nyrmax)
        integer ilimit(nvarmax)

        do j=1,mnmax
        do i1=1,nvarmax
        do i2=1,nvarmax
        cobs(j,i1,i2)=0.0
        dobs(j,i1,i2)=0.0
        enddo
        enddo
        enddo

        do j=1,nout
c	for corss correlations
        do i1=1,nvar
        do i2=1,nvar
        do 11 i=1,ny
        xx(i)=atm(i1,i,j)
        yy(i)=atm(i2,i,j)
11      continue
        call avsdcor(xx,yy,ny,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        m0(j,i1,i2)=cor
c	for cross correlation of previous month/season
        if(j.eq.1)then
        ii=0
        do i=2,ny
        ii=ii+1
        xx(ii)=atm(i1,i-1,nout)
        yy(ii)=atm(i2,i-1,nout)
        enddo
        else
        ii=0
        do i=1,ny
        ii=ii+1
        xx(ii)=atm(i1,i,j-1)
        yy(ii)=atm(i2,i,j-1)
        enddo
        endif
        call avsdcor(xx,yy,ii,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        m0p(j,i1,i2)=cor

c	for lagged cross correlations
        if(j.eq.1)then
        ii=0
        do i=2,ny
        ii=ii+1
        xx(ii)=atm(i1,i,j)
        yy(ii)=atm(i2,i-1,nout)
        enddo
        else
        ii=0
        do i=1,ny
        ii=ii+1
        xx(ii)=atm(i1,i,j)
        yy(ii)=atm(i2,i,j-1)
        enddo
        endif
        call avsdcor(xx,yy,ii,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        m1(j,i1,i2)=cor
        enddo
        enddo
        enddo

        do j=1,nout
        do i1=1,nvar
        do i2=1,nvar
        go1(i1,i2)=0.0
        co1(i1,i2)=0.0
        if(i1.eq.i2)go1(i1,i2)=1.0
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        if(i1.eq.i2)co1(i1,i2)=m1(j,i1,i2)
        go1(i1,i2)=m0(j,i1,i2)-m1(j,i1,i1)*m0p(j,i1,i2)*m1(j,i2,i2)
        enddo
        enddo

        call sqroot(go1,nvar,nvarmax)
        do i1=1,nvar
        do i2=1,nvar
        cobs(j,i1,i2)=co1(i1,i2)
        dobs(j,i1,i2)=go1(i1,i2)
        enddo
        enddo

        if(inx.eq.2)then
        temp=0.0
        do i1=1,nvar
        do i2=1,nvar
        temp(i1,i2)=go1(i2,i1)
        enddo
        enddo
        m1t=0.0
        call matmat(temp,go1,m1t,nvar,nvarmax)
        call solve(M1t,nvar,nvarmax)
        go1=0.0
        call matmat(m1t,temp,go1,nvar,nvarmax)
c
        do i1=1,nvar
        do i2=1,nvar
        dobs(j,i1,i2)=go1(i1,i2)
        enddo
        enddo
        endif
        enddo
        return
        end
c****************************************************************
      subroutine avsdy(rf,nvar,ny,avy,sdy,rhoy,amn,amx,nyrmax,nvarmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 rf(nvarmax,nyrmax),avy(nvarmax),sdy(nvarmax)
        real*4 rhoy(nvarmax),amn(nvarmax),amx(nvarmax)
        real*4 xx(nyrmax),yy(nyrmax),zz(nyrmax)
        do i=1,nvarmax
        avy(i)=0.0
        sdy(i)=0.0
        rhoy(i)=0.0
        enddo

        do il=1,nvar
        amx(il)=-10000.0
        amn(il)=10000.0
        do i=1,ny
        if(rf(il,i).gt.amx(il))amx(il)=rf(il,i)
        if(rf(il,i).lt.amn(il))amn(il)=rf(il,i)
        enddo

c	calculate annual means and sd
        do i=1,ny
        zz(i)=rf(il,i)
        enddo
        call basic(zz,avm,sdm,ny)
        if(sdm.lt.0.01)sdm=0.01
        sdy(il)=sdm
        avy(il)=avm

        do i=2,ny
        xx(i-1)=zz(i)
        yy(i-1)=zz(i-1)
        enddo
        call avsdcor(xx,yy,ny-1,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.gt.1.0)cor=1.0
        rhoy(il)=cor
c	if(il.eq.1)write(15,*)il,j,avm,sdm
        enddo
        return
        end
c****************************************************************
      subroutine c_g_crs_year(rf,nvar,ny,crs,inx,
     1                        ilimit,thres,nyrmax,nvarmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 rf(nvarmax,nyrmax),thres(nvarmax)
        real*4 t1(nvarmax,nvarmax),t2(nvarmax,nvarmax)
        real*4 crs(nvarmax,nvarmax)
        real*4 xx(nyrmax),yy(nyrmax)
        integer ilimit(nvarmax)

        do i=1,nvarmax
        do j=1,nvarmax
        crs(i,j)=0.0
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
c	calculate correlations
        do i=1,ny
        xx(i)=rf(i1,i)
        yy(i)=rf(i2,i)
        enddo
        call avsdcor(xx,yy,ny,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        crs(i1,i2)=cor
        enddo
        enddo
        call sqroot(crs,nvar,nvarmax)
        if(inx.eq.2)then
        temp=0.0
        do i1=1,nvar
        do i2=1,nvar
        t1(i1,i2)=crs(i2,i1)
        enddo
        enddo
        m0=0.0

        call matmat(t1,crs,t2,nvar,nvarmax)
        call solve(t2,nvar,nvarmax)
        crs=0.0
        call matmat(t2,t1,crs,nvar,nvarmax)
        endif

        return
        end
c****************************************************************
      subroutine c_g_corl_year(rf,nvar,ny,cobs,dobs,inx,
     1                         ilimit,thres,nyrmax,nvarmax)
c	finds out monthly mean, sd and corl of Given series
        real*4 rf(nvarmax,nyrmax),thres(nvarmax),
     1       cobs(nvarmax,nvarmax),dobs(nvarmax,nvarmax)
        real*4 temp(nvarmax,nvarmax),m1t(nvarmax,nvarmax)
        real*4 m0(nvarmax,nvarmax),m1(nvarmax,nvarmax)
        real*4 co1(nvarmax,nvarmax),go1(nvarmax,nvarmax)
        real*4 xx(nyrmax),yy(nyrmax)
        integer ilimit(nvarmax)

        do i=1,nvarmax
        do j=1,nvarmax
        cobs(i,j)=0.0
        dobs(i,j)=0.0
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
c	calculate correlations
        do i=1,ny
        xx(i)=rf(i1,i)
        yy(i)=rf(i2,i)
        enddo
        call avsdcor(xx,yy,ny,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
        m0(i1,i2)=cor
        do i=2,ny
        xx(i-1)=rf(i1,i)
        yy(i-1)=rf(i2,i-1)
        enddo
        call avsdcor(xx,yy,ny-1,cor,avm,avm1,sdm,sdm1,nyrmax)
        if(cor.ge.1.0)cor=0.9999
c	cor=0.5*(log(1+cor)-log(1-cor))
        m1(i1,i2)=cor
c	if(il.eq.1)write(15,*)il,j,avm,sdm
        enddo
        enddo

        do i1=1,nvar
        do i2=1,nvar
        co1(i1,i2)=0.0
        go1(i1,i2)=0.0
        if(i1.eq.i2)go1(i1,i2)=1.0
        enddo
        enddo
        do i1=1,nvar
        do i2=1,nvar
        if(i1.eq.i2)co1(i1,i2)=m1(i1,i2)
        go1(i1,i2)=m0(i1,i2)*(1.0-m1(i1,i1)*m1(i2,i2))
        enddo
        enddo
        call sqroot(go1,nvar,nvarmax)
        do i1=1,nvar
        do i2=1,nvar
        cobs(i1,i2)=co1(i1,i2)
        dobs(i1,i2)=go1(i1,i2)
        enddo
        enddo

        if(inx.eq.2)then
        temp=0.0
        do i1=1,nvar
        do i2=1,nvar
        temp(i1,i2)=go1(i2,i1)
        enddo
        enddo
        m0=0.0

        call matmat(temp,go1,m0,nvar,nvarmax)
        call solve(m0,nvar,nvarmax)
        go1=0.0
        call matmat(m0,temp,go1,nvar,nvarmax)
        do i1=1,nvar
        do i2=1,nvar
        dobs(i1,i2)=go1(i1,i2)
        enddo
        enddo
        endif

        return
        end
c****************************************************************
      subroutine day(nday,monmax)
        integer nday(monmax)
        nday(1)=31
        nday(2)=29
        nday(3)=31
        nday(4)=30
        nday(5)=31
        nday(6)=30
        nday(7)=31
        nday(8)=31
        nday(9)=30
        nday(10)=31
        nday(11)=30
        nday(12)=31
        return
      end
c**************************************************************
      SUBROUTINE avsdcor(xx,yy,n,r,ax,ay,sx,sy,nmax)
        INTEGER n
        real*4 xx(nmax),yy(nmax)
        REAL r,TINY
        PARAMETER (TINY=1.e-20)
        INTEGER j
        REAL ax,ay,sxx,sxy,syy,t,xt,yt,sx,sy,an
        an=float(n)
        ax=0.
        ay=0.
        do 11 j=1,n
            ax=ax+xx(j)
            ay=ay+yy(j)
11      continue
        ax=ax/n
        ay=ay/n
        sxx=0.
        syy=0.
        sxy=0.
        do 12 j=1,n
            xt=xx(j)-ax
            yt=yy(j)-ay
            sxx=sxx+xt**2
            syy=syy+yt**2
            sxy=sxy+xt*yt
12      continue
        r=sxy/(sqrt(sxx*syy)+TINY)
        sx=sqrt(sxx/an)
        sy=sqrt(syy/an)
        return
        end
cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
      SUBROUTINE MATMAT(A,B,C,n,nmax)
        REAL*4 A(nmax,nmax),B(nmax,nmax),C(nmax,nmax)
        C=0.0
        DO 111 I=1,N
        DO 112 J=1,N
        C(I,J)=0.0
        DO 101 K=1,N
        C(I,J)=C(I,J)+A(I,K)*B(K,J)
101     CONTINUE
112     CONTINUE
111     CONTINUE
        RETURN
      END 
C ****************************************************************
      SUBROUTINE MATMUL (A,B,C,N,NMAX)
        DIMENSION A(NMAX,NMAX),B(NMAX),C(NMAX)
        C=0.0
        DO 10 I=1,N
        C(I)=0.0
        DO 20 J=1,N
        C(I)=C(I)+A(I,J)*B(J)
20      CONTINUE
10      CONTINUE
        RETURN
      END
C ****************************************************************
      subroutine solve(sss,nv,nvrmax)
        Implicit double precision (A-H,O-Z) 
        real*4 sss(nvrmax,nvrmax)        
            real*8 det,tol
        real*8 ss(nvrmax,nvrmax)        
            real*8 u(nvrmax,nvrmax),W(nvrmax),V(nvrmax,nvrmax)
        REAL*8 RV1(NVRMAX)
        LOGICAL MATU, MATV
        MATU = .TRUE.
        MATV = .TRUE.      

        tol = 1.d-0


        if(nv.eq.1)then
        det=sss(1,1)
        if(sss(1,1).ne.0.0)sss(1,1)=1.0/sss(1,1)
        return
        endif		 

        do i=1,nv
        do j=1,nv
        if(abs(sss(i,j)).lt.0.001.and.sss(i,j).gt.0.0)sss(i,j)=0.001
        if(abs(sss(i,j)).lt.0.001.and.sss(i,j).lt.0.0)sss(i,j)=-0.001
            ss(i,j)=dble(sss(i,j))
                if(i.eq.j)ss(i,j)=ss(i,j)+0.0001d0
        enddo
        enddo

    
C     Singular value decomposition
        CALL SVD(NVRMAX,NV,NV,SS,W,MATU,U,MATV,V,IERR,RV1)

C     Product V*(U-Transpose)*(1/W)
        do k = 1, nv
            do l = 1, nv
                ss(k,l)= 0.0
                do j = 1, nv
                if (w(j) .GT. TOL) THEN
                    ss(k,l) = ss(k,l) + v(k,j)*u(l,j)/w(j)
                ENDIF
                end do
            end do
        end do


c	calculate determinant
        det=1.d0
            do 220 i=1,nv
                det=det*w(i)
220         continue


        do i=1,nv
        do j=1,nv
        sss(i,j)=real(ss(i,j))
        enddo
        enddo

        return
      end
c
c
c****************************************************************
      subroutine sqroot(sss,nv,nvrmax)
        Implicit double precision (A-H,O-Z) 
        real*4 sss(nvrmax,nvrmax)
        real*8 ss(nvrmax,nvrmax),tol        
            real*8 u(nvrmax,nvrmax),W(nvrmax),V(nvrmax,nvrmax)
        REAL*8 RV1(NVRMAX)
        LOGICAL MATU, MATV
        MATU = .TRUE.
        MATV = .TRUE.      

        tol = 1.d-8

        if(nv.eq.1)then
        if(sss(1,1).ne.0.0)sss(1,1)=sqrt(sss(1,1))
        return
        endif		 


        do  i=1,nv
        do j=1,nv
        if(abs(sss(i,j)).lt.0.001.and.sss(i,j).gt.0.0)sss(i,j)=0.001
        if(abs(sss(i,j)).lt.0.001.and.sss(i,j).lt.0.0)sss(i,j)=-0.001
            ss(i,j)=dble(sss(i,j))
                if(i.eq.j)ss(i,j)=ss(i,j)+0.0001d0
        enddo
        enddo
        CALL SVD(NVRMAX,NV,NV,SS,W,MATU,U,MATV,V,IERR,RV1)
C     Product V*(U-Transpose)*sqrt(W)
        do k = 1, nv
            do l = 1, nv
                ss(k,l)= 0.d0
                do j = 1, nv
                if (w(j) .GT. TOL) THEN
                    ss(k,l) = ss(k,l) + v(k,j)*u(l,j)*dsqrt(w(j))
                ENDIF
                end do
            end do
        end do
        do i=1,nv
            do j=1,nv
                sss(i,j)=real(ss(i,j))
            enddo
        enddo
        return
        end
c
C---------------------------------------------------------------------
C
      SUBROUTINE SVD(NM,M,N,A,W,MATU,U,MATV,V,IERR,RV1)
C
C                               SVD
C
C---------------------------------------------------------------------
C
        IMPLICIT DOUBLE PRECISION (A-H,O-Z)
        REAL*8 MACHEP
C
C
        DIMENSION A(NM,*),U(NM,*),V(NM,*),W(*),RV1(*)
C
        LOGICAL MATU,MATV
C
C     THIS SUBROUTINE IS A TRANSLATION OF THE ALGOL PROCEDURE SVD,
C     HANDBOOK AUTO COMP VOL II LINEAR ALGEBRA P.134-151
C
C     THIS SUBROUTINE DETERMINES THE SINGULAR VALUE DECOMPOSITION
C     A=UDV' OF A REAL M BY N MATRIX. HOUSEHOLDER
C     BIDIAGONALISATION AND A VARIANT OF THE QR ALGORITHM ARE USED
C
C
C
C  ON INPUT:
C
C  NM1   MUST BE SET TO THE ROW DIMENSION OF TWO DIMENSIONAL
C        ARRAY PARAMETER A AS DECLARED IN THE CALLING PROGRAM
C        DIMENSION STATEMENT. NOTE THAT NM1 MUST BE AT LEAST
C        AS LARGE AS M
C
C  NM2   MUST BE SET TO THE ROW DIMENSION OF TWO DIMENSIONAL
C        ARRAY PARAMETER U AS DECLARED IN THE CALLING PROGRAM
C        DIMENSION STATEMENT. NOTE THAT MN2 MUST BE AT LEAST
C        AS LARGE AS M.
C
C  NM3   MUST BE SET TO THE ROW DIMENSION OF TWO DIMENSIONAL
C        ARRAY PARAMETER V AS DECLARED IN THE CALLING PROGRAM
C        DIMENSION STATEMENT. NOTE THAT NM3 MUST BE AT LEAST
C        AS LARGE AS N
C
C  M     IS THE NUMBER OF ROWS OF A (AND U)
C
C  N     IS THE NUMBER OF COLUMNS OF A (AND U) AND THE ORDER OF V
C
C  A     IS THE INPUT MATRIX TO BE DECOMPOSED
C
C  MATU  IS SET TO .TRUE. IF THE U MATRIX IN THE
C        DECOMPOSITION IS DESIRED AND TO FALSE OTHERWISE
C
C  MATV  IS SET TO .TRUE. IF THE V MATRIX IN THE DECOMPOSITION
C        IS DESIRED AND TO FALSE OTHERWISE
C
C
C
C  ON OUTPUT:
C
C  A     IS UNALTERED (UNLESS OVERWRITTEN BY U)
C
C  W     CONTAINS THE N NON-NEGATIVE SINGULAR VALUES OF A (THE
C        DIAGONAL ELEMENTS OF D.)  IF AN ERROR EXIT IS MADE THE
C        SINGULAR VALUES SHOULD BE CORRECT FOR INDICES
C        IERR+1,IERR+2,...........,N
C
C  U     CONTAINS THE MATRIX U (ORTHOGONAL COLUMN VECTORS) OF THE
C        DECOMPOSITION IF MATU HAS BEEN SET TO .TRUE. OTHERWISE
C        U IS USED AS A TEMPORARY ARRAY. U MAY COINCIDE WITH A.
C        IF AN ERROR EXIT IS MADE, THE COLUMNS OF U CORRESPONDING TO
C        INDICES OF CORRECT SINGULAR VALUES SHOULD BE CORRECT.
C
C  V     CONTAINS THE MATRIX V (ORTHOGONAL) OF THE DECOMPOSITION IF
C        MATV HAS BEEN SET TO .TRUE. OTHERWISE V IS NOT REFERENCED.
C        V MAY ALSO COINCIDE WITH A. IF AN ERROR EXIT IS MADE
C        THE COLUMNS OF V CORRESPONDING TO INDICES OF CORRECT
C        SINGULAR VALUES SHOULD BE CORRECT.
C
C  IERR  IS SET TO
C          0 - FOR NORMAL RETURN
C          K - IF THE K-TH VALUE HAS NOT BEEN
C              DETERMINED AFTER 30 ITERATIONS
C
C  RV1   IS A TEMPORARY STORAGE ARRAY
C
C      ************************************************************
C
C  MACHEP IS A MACHINE DEPENDENT PARAMETER SPECIFYING
C         THE RELATIVE PRECISION OF FLOATING POINT ARITHMETIC
C
        DATA MACHEP/1.D-10/
C
        IERR = 0
        DO 17 I=1,M
            DO 27 J=1,N
            U(I,J) = A(I,J)
27      CONTINUE
17      CONTINUE
C
C **** HOUSEHOLDER TRANSFORMATION TO BIDIAGONAL FORM ******
C
        SCALE = 0.0
        X = 0.0
        G = 0.0
        DO 300 I=1,N
            L = I + 1
            RV1(I) = SCALE*G
            G = 0.0
            S = 0.0
            SCALE = 0.0
            IF (I .GT. M) GO TO 210
            DO 37 K=I,M
            SCALE = SCALE + ABS(U(K,I))
37      CONTINUE
        IF (SCALE .EQ. 0.0) GO TO 210
        DO 47 K=I,M
            U(K,I) = U(K,I)/SCALE
            S = S + U(K,I)**2
47      CONTINUE
        F = U(I,I)
        XZ = SQRT(S)
        G = -SIGN (XZ,F)
        H = F*G - S
        U(I,I) = F - G
        IF (I .EQ. N) GO TO 190
        DO 57 J=L,N
            S = 0.0
            DO 67 K=I,M
                S = S + U(K,I)*U(K,J)
67         CONTINUE
            F = S/H
            DO 77 K=I,M
            U(K,J) = U(K,J) + F*U(K,I)
77      CONTINUE
57      CONTINUE
190     DO 87 K=I,M
          U(K,I) = SCALE*U(K,I)
87      CONTINUE
210     W(I) = SCALE*G
        SCALE = 0.0
        S = 0.0
        G = 0.0
        IF (I .GT. M .OR. I .EQ. N) GO TO 290
        DO 97 K=L,N
          SCALE = SCALE + ABS(U(I,K))
97      CONTINUE
        IF (SCALE .EQ. 0.0) GO TO 290
        DO 107 K=L,N
          U(I,K) = U(I,K)/SCALE
          S = S + U(I,K)**2
107     CONTINUE
        F = U(I,L)
        XZ = SQRT(S)
        G = -SIGN(XZ,F)
        H = F*G-S
        U(I,L) = F - G
        DO 117 K=L,N
          RV1(K) = U(I,K)/H
117     CONTINUE
        IF (I .EQ. M) GO TO 270
        DO 127 J=L,M
          S = 0.0
          DO 137 K=L,N
            S = S + U(J,K)*U(I,K)
137         CONTINUE
          DO 147 K=L,N
            U(J,K) = U(J,K) +S*RV1(K)
147     CONTINUE
127     CONTINUE
270     DO 157 K=L,N
          U(I,K) = SCALE*U(I,K)
157     CONTINUE
290     XZ = ABS(W(I)) + ABS(RV1(I))
        X = MAX(X,XZ)
300     CONTINUE
C
C **** ACCUMULATION OF RIGHT-HAND TRANSFORMATIONS ****
C
        IF (.NOT. MATV) GO TO 410
        DO 400 II=1,N
        I = N + 1 - II
        IF (I .EQ. N) GO TO 390
        IF (G .EQ. 0.0) GO TO 360
        DO 167 J=L,N
C
C **** DOUBLE DIVISION AVOIDS POSSIBLE UNDERFLOW *****
C
        V(J,I) = (U(I,J)/U(I,L))/G
167     CONTINUE
        DO 177 J=L,N
          S = 0.0
          DO 187 K=L,N
            S = S + U(I,K)*V(K,J)
187      CONTINUE
          DO 197 K=L,N
            V(K,J) = V(K,J) + S*V(K,I)
197      CONTINUE
177    CONTINUE
360    DO 207 J=L,N
          V(I,J) = 0.0
          V(J,I) = 0.0
207     CONTINUE
390     V(I,I) = 1.0
        G = RV1(I)
        L = I
400     CONTINUE
C
C ***  ACCUMULATION OF LEFT-HAND TRANSFORMATIONS *********
C
410     IF ( .NOT. MATU) GO TO 510
        MN = N
        IF (M .LT. N) MN = M
        DO 500 II=1,MN
        I = MN+1-II
        L = I+1
        G = W(I)
        IF (I .EQ. N) GO TO 430
        DO 217 J=L,N
          U(I,J) = 0.0
217     CONTINUE
430     IF (G .EQ. 0.0 ) GO TO 475
        IF (I .EQ. MN) GO TO 460
        DO 227 J=L,N
          S = 0.0
          DO 237 K=L,M
            S = S+U(K,I)*U(K,J)
237     CONTINUE
C
C ***** DOUBLE DIVISION AVOIDS POSSIBLE UNDERFLOW ******
C
          F = (S/U(I,I))/G
          DO 247 K=I,M
            U(K,J) = U(K,J)+F*U(K,I)
247     CONTINUE
227     CONTINUE
460     DO 257 J=I,M
          U(J,I) = U(J,I)/G
257     CONTINUE
        GO TO 490
475     DO 267 J=I,M
          U(J,I) = 0.0
267     CONTINUE
490     U(I,I) = U(I,I)+1.0
500     CONTINUE
C
C ***** DIAGONALISATION OF THE BIDIAGONAL FORM **********
C
510     EPS = MACHEP*X
C
C ***** FOR K=N STEP -1 UNTIL 1 DO **********
C
        DO 700 KK=1,N
            K1 = N-KK
            K = K1+1
            ITS = 0
C
C ****** TEST FOR SPLITTING ******
C        FOR L=K STEP -1 UNTIL 1 DO
C
520     DO 277 LL=1,K
          L1 = K -LL
          L = L1+1
          IF (ABS(RV1(L)) .LE. EPS) GO TO 565
C
C         RV1(I) IS ALWAYS ZERO, SO THERE IS NO EXIT
C         THROUGH THE BOTTOM OF THE LOOP
C
        IF (ABS(W(L1)) .LE. EPS) GO TO 540
277     CONTINUE
540     C = 0.0
        S = 1.0
        DO 560 I=L,K
          F = S*RV1(I)
          RV1(I) = C*RV1(I)
          IF (ABS(F) .LE. EPS) GO TO 565
          G = W(I)
          H = SQRT(F*F+G*G)
          W(I) = H
          C = G/H
          S = -F/H
          IF (.NOT.MATU) GOTO 560
            DO 550 J=1,M
              Y = U(J,L1)
              Z = U(J,I)
              U(J,L1) = Y*C+Z*S
              U(J,I) = -Y*S+Z*C
550         CONTINUE
560     CONTINUE
C
C       TEST FOR CONVERGENCE
C
565     Z = W(K)
        IF (L .EQ. K) GO TO 650
C
C       SHIFT FROM BOTTOM 2 BY 2 MINOR
C
        IF (ITS .EQ. 30) GO TO 1000
        ITS = ITS+1
        X = W(L)
        Y = W(K1)
        G = RV1(K1)
        H = RV1(K)
        F = ((Y-Z)*(Y+Z)+(G-H)*(G+H))/(2.0*H*Y)
        G = SQRT(F*F+1.0)
        F = ((X-Z)*(X+Z)+H*(Y/(F+SIGN(G,F))-H))/X
C
C       NEXT QR TRANSFORMATION
C
        C = 1.0
        S = 1.0
C
        DO 600 I1=L,K1
          I = I1+1
          G = RV1(I)
          Y = W(I)
          H = S*G
          G = C*G
          Z = SQRT(F*F+H*H)
          RV1(I1) = Z
          C = F/Z
          S = H/Z
          F = X*C+G*S
          G = -X*S+G*C
          H = Y*S
          Y = Y*C
          IF ( .NOT. MATV) GO TO 575
C
          DO 287 J=1,N
            X = V(J,I1)
            Z = V(J,I)
            V(J,I1) = X*C+Z*S
            V(J,I) = -X*S+Z*C
287      CONTINUE
C
575       Z = SQRT(F*F+H*H)
          W(I1) = Z
C
C         ROTATION CAN BE ARBITRARY IF Z IS ZERO
C
          IF (Z .EQ. 0.0) GO TO 580
          C = F/Z
          S = H/Z
580       F = C*G+S*Y
          X = -S*G+C*Y
          IF ( .NOT.MATU ) GOTO 600
            DO 297 J=1,M
              Y = U(J,I1)
              Z = U(J,I)
              U(J,I1) = Y*C+Z*S
              U(J,I) = -Y*S+Z*C
297        CONTINUE
600     CONTINUE
        RV1(L) = 0.0
        RV1(K) = F
        W(K) = X
        GO TO 520
C
C         CONVERGENCE
C
650     IF (Z .GE. 0.0) GOTO 700
C
C           W(K) IS MADE NON-NEGATIVE
C
          W(K) = -Z
          IF (.NOT.MATV) GOTO 700
C
            DO 307 J=1,N
              V(J,K) = -V(J,K)
307     CONTINUE
700     CONTINUE
C**************************************************************
C     TO FIND THE ZERO CUT OFF FOR SINGULAR VALUES
C**************************************************************
C
C     DUPLICATE SINGULAR VALUES
C
        DO 317 I=1,N
            RV1(I) = W(I)
317     CONTINUE
C
C     ORDER SINGULAR VALUES
C
        NEND = N-1
702     LAST = 0
        DO 703 I=1,NEND
        IF (RV1(I+1) .GT. RV1(I)) GOTO 703
          LAST = I
          TEMP = RV1(I+1)
          RV1(I+1) = RV1(I)
          RV1(I) = TEMP
703     CONTINUE
C
        NEND = LAST-1
        IF (NEND .GE. 1) GO TO 702
        NEND = N-1
        ITEST = 1
        EPS = SQRT(MACHEP)
        DO 327 I=1,NEND
        IF (ABS(RV1(I)).LT.EPS) GOTO 704
        TEST = RV1(I+1)/RV1(I)
        IF (TEST .LT. EPS .AND. RV1(I+1) .LT. EPS) GO TO 705
704     ITEST = I+1
327     CONTINUE
705     IF(ITEST .EQ. N) GO TO 1001
        ZERO = (RV1(I+1)+RV1(I))/2.
C
C     REPLACE ZERO FOR ZERO VALUES IN W
C
        DO 337 I=1,N
        IF (W(I) .LT. ZERO) W(I) = 0.
337     CONTINUE
        GO TO 1001
C
C ************  SET ERROR  --  NO CONVERGENCE TO A SINGULAR VALUE
C                               AFTER 30 ITERATIONS  *************************
C
1000    IERR = K
1001    RETURN
C----------------------------------------------------------------------------
        END
c----------------------------------------------------------------------------
c********************************************************************
      subroutine skew(x,n,nxmax,sk)
        dimension x(nxmax)
        real*8 a,b,dn,sum1,sum2,sum3,skk	
        sum1 = 0.d0
        sum2 = 0.d0
        sum3 = 0.d0
        sk   = 0.0
        skk=0.d0
        do 100 i=1,n
        xx=dble(x(i))
        sum1 = sum1 + xx
        sum2 = sum2 + xx*xx
        sum3 = sum3 + xx*xx*xx
 100    continue
        if(dabs(sum1).lt.0.01)return

        dn = dfloat(n)
        a = dn*dn*sum3 - 3.*dn*sum1*sum2 + 2.*sum1*sum1*sum1
        b = dn*dn*dn*((sum2-sum1*sum1/dn)/dn)**1.5
        if(b.gt.0.001d0)skk = a/b
        sk=real(skk)
c	call basic(x,av,sd,n)
c	write(*,*)av,sd,sk
c	pause
        return
        end
c***************************************************************
      subroutine adj_thres(rec,gcmc,gcmf,nvar,ns,ny,nsgc,ngc,ngf,
     1                     nsgf,miss,itime,nout,leap,idays,thres,
     2                     ilimit,nday,nvarmax,nyrmax,monmax,
     3                     ndmax,nnmax)
    
        real*4 rec(nvarmax,nyrmax,monmax,ndmax),
     1       gcmc(nvarmax,nyrmax,monmax,ndmax),
     2       gcmf(nvarmax,nyrmax,monmax,ndmax)
        real*4 xx(nnmax),thres(nvarmax)
        integer	ii(nnmax),jj(nnmax),ll(nnmax),nn(nnmax),
     1       idays(monmax,4),nday(monmax),leap(4),ilimit(nvarmax)

        do k=1,nvar
        if(ilimit(k).eq.0)goto 410
        do j=1,nout
c	count current climate/calibration data
        ithresh=0
        itoth=0
        ilpg=3
        thk=thres(k)

        do i=1,ny
        if(itime.ne.0)then
        nd=1
        else
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(ns,i,nd)
        else
        nd=idays(j,ilpg)
        endif
        endif
        do l=1,nd
        if(rec(k,i,j,l).gt.miss)then
        if(rec(k,i,j,l).ge.thk)ithresh=ithresh+1
        itoth=itoth+1
        endif
        enddo
        enddo
c	count current climate/calibration atm data
        ilpg=1
        ithresc=0
        itotc=0
        do i=1,ngc
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmc(k,i,j,l).gt.miss)then
        if(gcmc(k,i,j,l).ge.thk)ithresc=ithresc+1
        itotc=itotc+1
        endif
        enddo
        enddo
c	check if raw data has more instances of above threshold
        counth=float(ithresh)*100.0/float(itoth)
        countc=float(ithresc)*100.0/float(itotc)
c	if difference is less than 0.5% - no adjustment
        if(abs(countc-counth).lt.0.05)goto 400
c	scale atm data
        if(countc.gt.counth)goto 50
c	we have more above threshold instances in the raw data
c	pull down a few values close to the threshold
        if(countc.lt.counth)goto 150
c	we have less 'above threshold' instances in the raw data
c	identify randomly values below threshold and raise them to threshold
        goto 400

 50     continue
        step=0.10*thk
        iturn=0
        istp=0
        add=0.0

 100    iturn=iturn+1
        ithresc=0
        itotc=0
        ilpg=1
        add=add+step
        do i=1,ngc
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmc(k,i,j,l).gt.miss)then
        if(gcmc(k,i,j,l).ge.thk+add)ithresc=ithresc+1
        itotc=itotc+1
        endif
        enddo
        enddo
        countc=float(ithresc)*100.0/float(itotc)
        if(iturn.gt.1000)goto 300
c	if difference is less than 1% -we are fine
        if(abs(countc-counth).lt.0.050) go to 300
c	increase the threshold
        if(countc.gt.counth) then
        step=2*step
        goto 100
        endif
        if(countc.lt.counth) then
        add=add-step
        step=step/2.0
        goto 100
        endif
 300    continue
        ilpg=1
        do i=1,ngc
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmc(k,i,j,l).gt.miss)then
        if(gcmc(k,i,j,l).lt.thk+add.and.gcmc(k,i,j,l).ge.thk)
     1                 gcmc(k,i,j,l)=abs(ran1(iseed)*thk*0.05)
        endif
        enddo
        enddo
        ilpg=2
        do i=1,ngf
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmf(k,i,j,l).gt.miss)then
        if(gcmf(k,i,j,l).lt.thk+add.and.gcmf(k,i,j,l).ge.thk)
     1                 gcmf(k,i,j,l)=abs(ran1(iseed)*thk*0.05)
        endif
        enddo
        enddo
        goto 400


 150    continue
        iseed=-1875

c	for GCM current climate
        ii=0
        jj=0
        ll=0
        xx=0
        kk=0
        ilpg=1
        do i=1,ngc
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmc(k,i,j,l).gt.miss)then
        if(gcmc(k,i,j,l).lt.thk)then
        kk=kk+1
        ii(kk)=i
        jj(kk)=j
        ll(kk)=l
        xx(kk)=gcmc(k,i,j,l)
        endif
        endif
        enddo
        enddo
        ntot=(counth-countc)*float(itotc)/100.0
        if(ntot.gt.kk)ntot=kk
        nn=0
        do i=1,ntot
        mm=0
 30     n=int(ran1(iseed)*float(kk)+0.5)
        mm=mm+1
        if(mm.gt.kk)goto 35
        if(n.lt.1)goto 30
        if(n.gt.kk)goto 30
        if(i.gt.1)then
        do i1=1,i-1
        if(n.eq.nn(i1))goto 30
        enddo
        endif
        nn(i)=n		
        gcmc(k,ii(n),jj(n),ll(n))=thk+ran1(iseed)*thk
        enddo
 35     continue

c	for GCM future climate
        ii=0
        jj=0
        ll=0
        xx=0
        kk=0
        ilpg=2
        do i=1,ngf
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgf,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmf(k,i,j,l).gt.miss)then
        if(gcmf(k,i,j,l).lt.thk)then
        kk=kk+1
        ii(kk)=i
        jj(kk)=j
        ll(kk)=l
        xx(kk)=gcmf(k,i,j,l)
        endif
        endif
        enddo
        enddo
        ntot=(counth-countc)*itotc/100.0
        if(ntot.ge.0.9*kk)ntot=kk*0.9
        if(kk.eq.0)goto 400
        nn=0
        do i=1,ntot
        mm=0
 20     n=int(ran1(iseed)*float(kk)+0.5)
        mm=mm+1
        if(mm.gt.kk)goto 400
        if(n.lt.1)goto 20
        if(n.gt.kk)goto 20
        if(i.gt.1)then
        do i1=1,i-1
        if(n.eq.nn(i1))goto 20
        enddo
        endif
        nn(i)=n		
        gcmf(k,ii(n),jj(n),ll(n))=thk+ran1(iseed)*thk
        enddo

 400    continue
        itotc=0
        ithresc=0
        ilpg=1
        do i=1,ngc
        if(itime.ne.0)then
        nd=1
        else
        nd=idays(j,ilpg)
        if(leap(ilpg).eq.0)then
        nd=nday(j)
        if(j.eq.2)call daycount(nsgc,i,nd)
        endif
        endif
        do l=1,nd
        if(gcmc(k,i,j,l).gt.miss)then
        if(gcmc(k,i,j,l).ge.thk)ithresc=ithresc+1
        itotc=itotc+1
        endif
        enddo
        enddo
        countc=float(ithresc)*100.0/float(itotc)
        enddo
 410    continue
        enddo
        return
        end

c******************************************************
      FUNCTION ran1(idum)
c     calculates a random number in the range 0-1       
c      Implicit double precision (A-H,O-Z) 
        INTEGER idum,IA,IM,IQ,IR,NTAB,NDIV  
        REAL ran1,AM,EPS,RNMX  
        PARAMETER (IA=16807,IM=2147483647,AM=1./IM,IQ=127773,IR=2836,
     1          NTAB=32,NDIV=1+(IM-1)/NTAB,EPS=1.2e-7,RNMX=1.-EPS)
        INTEGER j,k,iv(NTAB),iy  
        SAVE iv,iy  
        DATA iv /NTAB*0/, iy /0/  
        if (idum.le.0.or.iy.eq.0) then  
        idum=max(-idum,1)  
        do 11 j=NTAB+8,1,-1  
        k=idum/IQ  
        idum=IA*(idum-k*IQ)-IR*k  
        if (idum.lt.0) idum=idum+IM  
        if (j.le.NTAB) iv(j)=idum  
 11     continue  
        iy=iv(1)  
        endif  
        k=idum/IQ  
        idum=IA*(idum-k*IQ)-IR*k  
        if (idum.lt.0) idum=idum+IM  
        j=1+iy/NDIV  
        iy=iv(j)  
        iv(j)=idum  
        ran1=min(AM*iy,RNMX)  
        return  
      END  
c**********************************************************
